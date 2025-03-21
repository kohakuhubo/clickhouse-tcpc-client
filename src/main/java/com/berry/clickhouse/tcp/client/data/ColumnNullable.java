package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeNullable;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ColumnNullable extends AbstractColumn {

    private final List<Byte> nullableSign;
    private final IColumn data;

    public ColumnNullable(String name, DataTypeNullable type, byte[] nameBytes, Object[] values) {
        super(name, type, nameBytes, values);
        nullableSign = new ArrayList<>();
        data = ColumnFactory.createColumn(null, type.getNestedDataType(), nameBytes, null);
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        if (object == null) {
            nullableSign.add((byte) 1);
            data.write(type.defaultValue());
        } else {
            nullableSign.add((byte) 0);
            data.write(object);
        }
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean immediate) throws IOException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name);
            serializer.writeUTF8StringBinary(type.name());
        }

        for (byte sign : nullableSign) {
            serializer.writeByte(sign);
        }

        if (immediate)
            buffer.writeTo(serializer);
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer);
        data.setColumnWriterBuffer(buffer);
    }

    @Override
    public void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory) {
        super.setColumnWriterBuffer(null);
        this.data.recycleColumnWriterBuffer(factory);
    }

    @Override
    public void rewind() {
        this.data.rewind();
    }
}
