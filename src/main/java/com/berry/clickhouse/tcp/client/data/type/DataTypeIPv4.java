package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;


public class DataTypeIPv4 implements IDataType<Long> {

    @Override
    public String name() {
        return "IPv4";
    }

    @Override
    public Long defaultValue() {
        return 0L;
    }

    @Override
    public Class<Long> javaType() {
        return Long.class;
    }

    @Override
    public void serializeBinary(Long data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeInt(data.intValue());
    }

    @Override
    public Long deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readInt() & 0xffffffffL;
    }

    @Override
    public String[] getAliases() {
        return new String[0];
    }

    @Override
    public Long deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().longValue() & 0xffffffffL;
    }
}
