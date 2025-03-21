package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeArray;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseArray;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ColumnArray extends AbstractColumn {

    private final List<Long> offsets;
    private final IColumn data;

    public ColumnArray(String name, DataTypeArray type, byte[] nameBytes, Object[] values) {
        super(name, type, nameBytes, values);
        offsets = new ArrayList<>();
        data = ColumnFactory.createColumn(null, type.getElemDataType(), nameBytes, null);
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        Object[] arr = ((ClickHouseArray) object).getArray();

        offsets.add(offsets.isEmpty() ? arr.length : offsets.get((offsets.size() - 1)) + arr.length);
        for (Object field : arr) {
            data.write(field);
        }
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean immediate) throws SQLException, IOException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name);
            serializer.writeUTF8StringBinary(type.name());
        }

        flushOffsets(serializer);
        data.flushToSerializer(serializer, false);

        if (immediate) {
            buffer.writeTo(serializer);
        }
    }

    public void flushOffsets(BinarySerializer serializer) throws IOException {
        for (long offsetList : offsets) {
            serializer.writeLong(offsetList);
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer);
        data.setColumnWriterBuffer(buffer);
    }

    @Override
    public void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory) {
        this.setColumnWriterBuffer(null);
        data.recycleColumnWriterBuffer(factory);
    }

    @Override
    public void rewind() {
        this.data.rewind();
    }

    @Override
    public void clear() {
        super.clear();
        offsets.clear();
        data.clear();
    }
}
