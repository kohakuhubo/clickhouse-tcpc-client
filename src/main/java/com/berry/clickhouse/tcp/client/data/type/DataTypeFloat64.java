package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * DataTypeFloat64类实现了IDataType接口
 * 表示ClickHouse中的Float64类型，使用Double表示
 */
public class DataTypeFloat64 implements IDataType<Double> {

    @Override
    public String name() {
        return "Float64"; // 返回数据类型名称
    }

    @Override
    public Double defaultValue() {
        return 0.0D; // 返回默认值
    }

    @Override
    public Class<Double> javaType() {
        return Double.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(Double data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeDouble(data); // 序列化为二进制格式
    }

    @Override
    public Double deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readDouble(); // 从二进制流反序列化
    }

    @Override
    public boolean isSigned() {
        return true; // Float64是有符号的
    }
}
