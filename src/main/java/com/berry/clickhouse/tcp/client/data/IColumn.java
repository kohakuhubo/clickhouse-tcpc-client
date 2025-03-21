package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public interface IColumn {

    boolean isExported();

    String name();

    IDataType<?> type();

    Object value(int idx);

    void setValues(Object[] values);

    void write(Object object) throws IOException, SQLException;

    void read(int rows, BinaryDeserializer binaryDeserializer) throws IOException, SQLException;

    void flushToSerializer(BinarySerializer serializer, boolean now) throws IOException, SQLException;

    void clear();

    void setColumnWriterBuffer(ColumnWriterBuffer buffer);

    default void setColumnWriterBuffer(ColumnWriterBuffer buffer, ColumnWriterBufferFactory factory) {
        setColumnWriterBuffer(buffer);
    }

    void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory);

    ColumnWriterBuffer getColumnWriterBuffer();

    int rowCnt();

    int addRowCnt();

    int addRowCnt(int count);

    void rewind();

    Object[] values();
}


