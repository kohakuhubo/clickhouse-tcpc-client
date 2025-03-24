package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

import java.time.LocalDate;

/**
 * DataTypeDate32类实现了IDataType接口
 * 表示ClickHouse中的Date32类型，使用LocalDate表示
 */
public class DataTypeDate32 implements IDataType<LocalDate> {

    private static final LocalDate DEFAULT_VALUE = LocalDate.of(1925, 1, 1); // 默认值

    public DataTypeDate32() {
    }

    @Override
    public String name() {
        return "Date32"; // 返回数据类型名称
    }

    @Override
    public LocalDate defaultValue() {
        return DEFAULT_VALUE; // 返回默认值
    }

    @Override
    public Class<LocalDate> javaType() {
        return LocalDate.class; // 返回对应的Java类
    }

    @Override
    public void serializeBinary(LocalDate data, BinarySerializer serializer) throws SQLException, IOException {
        long epochDay = data.toEpochDay(); // 将LocalDate转换为自1970-01-01以来的天数
        serializer.writeInt((int) epochDay); // 序列化为二进制格式
    }

    @Override
    public LocalDate deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        int epochDay = deserializer.readInt(); // 从二进制流反序列化
        return LocalDate.ofEpochDay(epochDay); // 返回LocalDate
    }
}
