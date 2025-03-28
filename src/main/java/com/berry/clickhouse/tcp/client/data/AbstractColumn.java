package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

public abstract class AbstractColumn implements IColumn {

    protected final String name;
    protected final IDataType<?> type;
    protected byte[] nameBytes;
    protected Object[] values;
    protected ColumnWriterBuffer buffer;
    protected int rowCnt = 0;

    public AbstractColumn(String name, IDataType<?> type, Object[] values) {
        this.name = name;
        this.type = type;
        this.values = values;
    }

    public AbstractColumn(String name, IDataType<?> type, byte[] nameBytes, Object[] values) {
        this.name = name;
        this.nameBytes = nameBytes;
        this.type = type;
        this.values = values;
    }

    @Override
    public boolean isExported() {
        return name != null;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public IDataType<?> type() {
        return type;
    }

    @Override
    public Object value(int idx) {
        return values[idx];
    }

    @Override
    public void clear() {
        values = new Object[0];
        if (null != buffer) {
            buffer.clear();
        }
        this.rowCnt = 0;
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public ColumnWriterBuffer getColumnWriterBuffer() {
        return buffer;
    }

    @Override
    public void setValues(Object[] values) {
        this.values = values;
    }

    @Override
    public void read(int rows, BinaryDeserializer binaryDeserializer) throws IOException, SQLException {
        this.type.deserializeBinaryBulk(rows, binaryDeserializer);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException, SQLException {
        type().serializeBinary(bytes, offset, length, buffer.column);
        addRowCnt();
    }

    @Override
    public void writeInt(byte[] bytes, int offset, int length, boolean isLittleEndian) throws IOException, SQLException {
        type().serializeIntBinary(bytes, offset, length, isLittleEndian, buffer.column);
        addRowCnt();
    }

    @Override
    public void write(byte byt) throws IOException, SQLException {
        type().serializeByteBinary(byt, buffer.column);
        addRowCnt();
    }

    @Override
    public int rowCnt() {
        return this.rowCnt;
    }

    @Override
    public int addRowCnt() {
        return ++this.rowCnt;
    }

    @Override
    public int addRowCnt(int count) {
        this.rowCnt += count;
        return this.rowCnt;
    }

    @Override
    public Object[] values() {
        return values;
    }
}
