package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeTuple;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseStruct;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class ColumnTuple extends AbstractColumn {

    private final IColumn[] columnDataArray;

    public ColumnTuple(String name, DataTypeTuple type, byte[] nameBytes, Object[] values) {
        super(name, type, nameBytes, values);

        IDataType<?>[] types = type.getNestedTypes();
        columnDataArray = new IColumn[types.length];
        for (int i = 0; i < types.length; i++) {
            columnDataArray[i] = ColumnFactory.createColumn(null, types[i], nameBytes, null);
        }
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        ClickHouseStruct tuple = (ClickHouseStruct) object;
        for (int i = 0; i < columnDataArray.length; i++) {
            columnDataArray[i].write(tuple.getAttributes()[i]);
        }
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean now) throws SQLException, IOException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name);
            serializer.writeUTF8StringBinary(type.name());
        }

        for (IColumn data : columnDataArray) {
            data.flushToSerializer(serializer, true);
        }

        if (now) {
            buffer.writeTo(serializer);
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer, ColumnWriterBufferFactory factory) {
        super.setColumnWriterBuffer(buffer);

        for (IColumn data : columnDataArray) {
            data.setColumnWriterBuffer(factory.getBuffer(data));
        }
    }

    @Override
    public void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory) {
        if (null != buffer) {
            factory.recycleBuffer(buffer);
            super.setColumnWriterBuffer(null);
        }

        for (IColumn data : columnDataArray) {
            data.recycleColumnWriterBuffer(factory);
        }
    }

    @Override
    public void rewind() {
        this.buffer.rewind();
        for (IColumn column : columnDataArray) {
            column.rewind();
        }
    }

    @Override
    public void clear() {
        super.clear();
        if (null != this.buffer) {
            this.buffer.clear();
        }
        for (IColumn column : columnDataArray) {
            column.clear();
        }
    }
}
