package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.BytesHelper;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;

public class DataTypeUInt64 implements BaseDataTypeInt64<BigInteger>, BytesHelper {

    @Override
    public String name() {
        return "UInt64";
    }

    @Override
    public BigInteger defaultValue() {
        return BigInteger.ZERO;
    }

    @Override
    public Class<BigInteger> javaType() {
        return BigInteger.class;
    }

    @Override
    public void serializeBinary(BigInteger data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeLong(data.longValue());
    }

    @Override
    public BigInteger deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        long l = deserializer.readLong();
        return new BigInteger(1, getBytes(l));
    }

    @Override
    public String[] getAliases() {
        return new String[0];
    }

    @Override
    public BigInteger deserializeText(SQLLexer lexer) throws SQLException {
        return BigInteger.valueOf(lexer.numberLiteral().longValue());
    }
}
