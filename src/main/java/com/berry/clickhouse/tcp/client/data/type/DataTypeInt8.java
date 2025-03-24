package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * DataTypeInt8类实现了BaseDataTypeInt8接口
 * 表示ClickHouse中的Int8类型，使用Byte表示
 */
public class DataTypeInt8 implements BaseDataTypeInt8<Byte> {

    @Override
    public String name() {
        return "Int8"; // 返回数据类型名称
    }

    @Override
    public Byte defaultValue() {
        return 0; // 返回默认值
    }

    @Override
    public Class<Byte> javaType() {
        return Byte.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(Byte data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeByte(data); // 序列化为二进制格式
    }

    @Override
    public Byte deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        return deserializer.readByte(); // 从二进制流反序列化
    }

    @Override
    public boolean isSigned() {
        return true; // Int8是有符号的
    }
}
