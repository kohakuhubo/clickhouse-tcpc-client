package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * DataTypeIPv4类实现了IDataType接口
 * 表示ClickHouse中的IPv4类型，使用Long表示
 */
public class DataTypeIPv4 implements IDataType<Long> {

    @Override
    public String name() {
        return "IPv4"; // 返回数据类型名称
    }

    @Override
    public Long defaultValue() {
        return 0L; // 返回默认值
    }

    @Override
    public Class<Long> javaType() {
        return Long.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(Long data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeInt(data.intValue()); // 序列化为二进制格式
    }

    @Override
    public Long deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readInt() & 0xffffffffL; // 从二进制流反序列化
    }
}
