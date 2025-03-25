package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;

/**
 * DataTypeIPv6类实现了IDataType接口
 * 表示ClickHouse中的IPv6类型，使用BigInteger表示
 */
public class DataTypeIPv6 implements IDataType<BigInteger> {

    @Override
    public String name() {
        return "IPv6"; // 返回数据类型名称
    }

    @Override
    public int byteSize() {
        return Long.BYTES;
    }

    @Override
    public BigInteger defaultValue() {
        return BigInteger.ZERO; // 返回默认值
    }

    @Override
    public Class<BigInteger> javaType() {
        return BigInteger.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(BigInteger data, BinarySerializer serializer) throws SQLException, IOException {
        byte[] bytes = data.toByteArray(); // 将BigInteger转换为字节数组
        if (bytes.length > 16) {
            throw new SQLException("IPv6 representation exceeds 16 bytes."); // 验证字节数组长度
        }
        byte[] paddedBytes = new byte[16]; // 创建16字节的数组
        int offset = 16 - bytes.length; // 计算填充偏移量
        System.arraycopy(bytes, 0, paddedBytes, offset, bytes.length); // 填充字节数组
        serializer.writeBytes(paddedBytes, 0, paddedBytes.length); // 序列化为二进制格式
    }

    @Override
    public BigInteger deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        byte[] bytes = deserializer.readBytes(16); // 从二进制流反序列化
        return new BigInteger(1, bytes); // 返回BigInteger
    }

    private static String convertIPv6ToHexadecimalString(String ipv6) {
        return ipv6.replace(":", ""); // 转换IPv6地址为十六进制字符串
    }
}
