package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * DataTypeUInt8类实现了BaseDataTypeInt8接口
 * 表示ClickHouse中的UInt8类型，使用Short表示
 */
public class DataTypeUInt8 implements BaseDataTypeInt8<Short> {

    @Override
    public String name() {
        return "UInt8"; // 返回数据类型名称
    }

    @Override
    public Short defaultValue() {
        return 0; // 返回默认值
    }

    @Override
    public Class<Short> javaType() {
        return Short.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(Short data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeByte(data.byteValue()); // 序列化为二进制格式
    }

    @Override
    public Short deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        byte b = deserializer.readByte(); // 从二进制流反序列化
        return (short) (b & 0xff); // 返回UInt8值
    }

    @Override
    public boolean isSigned() {
        return false; // UInt8是无符号的
    }
}
