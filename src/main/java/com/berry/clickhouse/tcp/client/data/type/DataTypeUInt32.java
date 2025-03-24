package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * DataTypeUInt32类实现了BaseDataTypeInt32接口
 * 表示ClickHouse中的UInt32类型，使用Long表示
 */
public class DataTypeUInt32 implements BaseDataTypeInt32<Long> {

    @Override
    public String name() {
        return "UInt32"; // 返回数据类型名称
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
        int res = deserializer.readInt(); // 从二进制流反序列化
        return 0xffffffffL & res; // 返回UInt32值
    }

    @Override
    public boolean isSigned() {
        return false; // UInt32是无符号的
    }
}
