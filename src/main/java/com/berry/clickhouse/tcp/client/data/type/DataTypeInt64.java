package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeInt64 implements BaseDataTypeInt64<Long> {

    @Override
    public String name() {
        return "Int64";
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
        serializer.writeLong(data);
    }

    @Override
    public Long deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readLong();
    }

    @Override
    public String[] getAliases() {
        return new String[]{"BIGINT"};
    }

    @Override
    public Long deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().longValue();
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
