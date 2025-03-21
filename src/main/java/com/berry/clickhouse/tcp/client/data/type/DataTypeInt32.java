package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeInt32 implements BaseDataTypeInt32<Integer> {

    @Override
    public String name() {
        return "Int32";
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
        serializer.writeInt(data);
    }

    @Override
    public Integer deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readInt();
    }

    @Override
    public String[] getAliases() {
        return new String[]{"INT", "INTEGER"};
    }

    @Override
    public Integer deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().intValue();
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
