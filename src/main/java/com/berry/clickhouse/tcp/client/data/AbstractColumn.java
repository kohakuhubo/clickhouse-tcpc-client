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
    protected final boolean useSystemBuffer;

    public AbstractColumn(String name, IDataType<?> type, Object[] values, boolean useSystemBuffer) {
        this.name = name;
        this.type = type;
        this.values = values;
        this.useSystemBuffer = useSystemBuffer;
    }

    public AbstractColumn(String name, IDataType<?> type, byte[] nameBytes, Object[] values, boolean useSystemBuffer) {
        this.name = name;
        this.nameBytes = nameBytes;
        this.type = type;
        this.values = values;
        this.useSystemBuffer = useSystemBuffer;
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
    public IDataType type() {
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
    public boolean isUseSysBuffer() {
        return useSystemBuffer;
    }

    @Override
    public Object[] values() {
        return values;
    }
}
