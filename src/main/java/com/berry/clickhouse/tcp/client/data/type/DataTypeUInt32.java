package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeUInt32 implements BaseDataTypeInt32<Long> {

    @Override
    public String name() {
        return "UInt32";
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
        int res = deserializer.readInt();
        return 0xffffffffL & res;
    }

    @Override
    public Long deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().longValue();
    }
}
