package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeUInt16 implements BaseDataTypeInt16<Integer> {

    @Override
    public String name() {
        return "UInt16";
    }

    @Override
    public Integer defaultValue() {
        return 0;
    }

    @Override
    public Class<Integer> javaType() {
        return Integer.class;
    }

    @Override
    public void serializeBinary(Integer data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeShort(data.shortValue());
    }

    @Override
    public Integer deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        short s = deserializer.readShort();
        return s & 0xffff;
    }

    @Override
    public Integer deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().intValue();
    }
}
