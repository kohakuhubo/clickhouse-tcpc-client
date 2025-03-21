package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

import java.util.UUID;

public class DataTypeUUID implements IDataType<UUID> {

    @Override
    public String name() {
        return "UUID";
    }

    @Override
    public UUID defaultValue() {
        return null;
    }

    @Override
    public Class<UUID> javaType() {
        return UUID.class;
    }

    @Override
    public UUID deserializeText(SQLLexer lexer) throws SQLException {
        return UUID.fromString(lexer.stringLiteral());
    }

    @Override
    public void serializeBinary(UUID data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeLong(data.getMostSignificantBits());
        serializer.writeLong(data.getLeastSignificantBits());
    }

    @Override
    public UUID deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return new UUID(deserializer.readLong(), deserializer.readLong());
    }
}
