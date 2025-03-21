package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;


public class DataTypeIPv6 implements IDataType<BigInteger> {

    @Override
    public String name() {
        return "IPv6";
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
        byte[] bytes = data.toByteArray();
        if (bytes.length > 16) {
            throw new SQLException("IPv6 representation exceeds 16 bytes.");
        }
        byte[] paddedBytes = new byte[16];
        int offset = 16 - bytes.length;
        System.arraycopy(bytes, 0, paddedBytes, offset, bytes.length);
        serializer.writeBytes(paddedBytes, 0, paddedBytes.length);
    }

    @Override
    public BigInteger deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        byte[] bytes = deserializer.readBytes(16);
        return new BigInteger(1, bytes);
    }
    @Override
    public String[] getAliases() {
        return new String[0];
    }

    @Override
    public BigInteger deserializeText(SQLLexer lexer) throws SQLException {
        String ipv6String = convertIPv6ToHexadecimalString(lexer.stringLiteral());
        return new BigInteger(ipv6String, 16);
    }

    private static String convertIPv6ToHexadecimalString(String ipv6) {
        return ipv6.replace(":", "");
    }
}
