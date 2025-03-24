package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * DataTypeUInt16类实现了BaseDataTypeInt16接口
 * 表示ClickHouse中的UInt16类型，使用Integer表示
 */
public class DataTypeUInt16 implements BaseDataTypeInt16<Integer> {

    @Override
    public String name() {
        return "UInt16"; // 返回数据类型名称
    }

    @Override
    public Integer defaultValue() {
        return 0; // 返回默认值
    }

    @Override
    public Class<Integer> javaType() {
        return Integer.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(Integer data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeShort(data.shortValue()); // 序列化为二进制格式
    }

    @Override
    public Integer deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        short s = deserializer.readShort(); // 从二进制流反序列化
        return s & 0xffff; // 返回UInt16值
    }

    @Override
    public boolean isSigned() {
        return false; // UInt16是无符号的
    }
}
