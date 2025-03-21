package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BuffedReadWriter;
import com.berry.clickhouse.tcp.client.exception.NoDefaultValueException;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public interface IDataType<CK> {

    String name();

    default String[] getAliases() {
        return new String[0];
    }
    default CK defaultValue() {
        throw new NoDefaultValueException("Column[" + name() + "] doesn't has default value");
    }

    Class<CK> javaType();

    default boolean nullable() {
        return false;
    }

    default boolean isSigned() {
        return false;
    }

    default String serializeText(CK value) {
        return value.toString();
    }

    void serializeBinary(CK data, BinarySerializer serializer) throws SQLException, IOException;

    default void serializeBinaryBulk(CK[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (CK d : data) {
            serializeBinary(d, serializer);
        }
    }

    CK deserializeText(SQLLexer lexer) throws SQLException;

    CK deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException;

    default Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[] data = new Object[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = this.deserializeBinary(deserializer);
        }
        return data;
    }

    default void deserializeBinaryBulk(int rows, BuffedReadWriter buffedReadWriter, BinaryDeserializer deserializer) throws SQLException, IOException {
        throw new IOException();
    }

    default boolean isFixedLength() {
        return true;
    }
}
