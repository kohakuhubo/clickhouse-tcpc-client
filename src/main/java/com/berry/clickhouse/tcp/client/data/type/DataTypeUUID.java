package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

import java.util.UUID;

/**
 * DataTypeUUID类实现了IDataType接口
 * 表示ClickHouse中的UUID类型，使用UUID表示
 */
public class DataTypeUUID implements IDataType<UUID> {

    @Override
    public String name() {
        return "UUID"; // 返回数据类型名称
    }

    @Override
    public UUID defaultValue() {
        return null; // 返回默认值
    }

    @Override
    public Class<UUID> javaType() {
        return UUID.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(UUID data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeLong(data.getMostSignificantBits()); // 序列化为二进制格式
        serializer.writeLong(data.getLeastSignificantBits()); // 序列化为二进制格式
    }

    @Override
    public UUID deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return new UUID(deserializer.readLong(), deserializer.readLong()); // 从二进制流反序列化
    }
}
