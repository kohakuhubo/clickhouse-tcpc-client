package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeMap;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ColumnMap extends AbstractColumn {
    private final IColumn[] columnDataArray;

    private final List<Long> offsets;

    public ColumnMap(String name, DataTypeMap type, byte[] nameBytes, Object[] values, boolean useSystemBuffer) {
        super(name, type, nameBytes, values, useSystemBuffer);
        offsets = new ArrayList<>();
        IDataType<?>[] types = type.getNestedTypes();
        columnDataArray = new IColumn[types.length];
        for (int i = 0; i < types.length; i++) {
            columnDataArray[i] = ColumnFactory.createColumn(null, types[i], nameBytes, null, useSystemBuffer);
        }
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        if (object instanceof Map) {
            Map<?, ?> dataMap = (Map<?, ?>) object;
            offsets.add(offsets.isEmpty() ? dataMap.size() : offsets.get((offsets.size() - 1)) + dataMap.size());

            for (Object key : dataMap.keySet()) {
                columnDataArray[0].write(key);
            }
            for (Object value : dataMap.values()) {
                columnDataArray[1].write(value);
            }
        }

    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean now) throws IOException, SQLException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name);
            serializer.writeUTF8StringBinary(type.name());
        }

        flushOffsets(serializer);

        for (IColumn data : columnDataArray) {
            data.flushToSerializer(serializer, true);
        }

        if (now) {
            buffer.writeTo(serializer);
        }
    }

    public void flushOffsets(BinarySerializer serializer) throws IOException {
        for (long offsetList : offsets) {
            serializer.writeLong(offsetList);
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer, ColumnWriterBufferFactory factory) {
        super.setColumnWriterBuffer(buffer);

        for (IColumn data : columnDataArray) {
            data.setColumnWriterBuffer(factory.getBuffer(data.isUseSysBuffer()));
        }
    }

    @Override
    public void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory) {
        if (null != this.buffer) {
            factory.recycleBuffer(this.buffer);
            super.setColumnWriterBuffer(null);
        }
        for (IColumn data : columnDataArray) {
            data.recycleColumnWriterBuffer(factory);
        }
    }

    @Override
    public void rewind() {
        this.buffer.rewind();
        for (IColumn data : columnDataArray) {
            data.rewind();
        }
    }

    @Override
    public void clear() {
        offsets.clear();
        for (IColumn data : columnDataArray) {
            data.clear();
        }
    }
}
