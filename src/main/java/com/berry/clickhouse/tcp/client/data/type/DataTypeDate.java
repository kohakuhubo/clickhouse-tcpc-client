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
 * DataTypeDate类实现了IDataType接口
 * 表示ClickHouse中的日期类型，使用LocalDate表示
 */
public class DataTypeDate implements IDataType<LocalDate> {

    private static final LocalDate DEFAULT_VALUE = LocalDate.ofEpochDay(0); // 默认值为1970-01-01

    public DataTypeDate() {
    }

    @Override
    public String name() {
        return "Date"; // 返回数据类型名称
    }

    @Override
    public int byteSize() {
        return Short.BYTES;
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
        serializer.writeShort((short) epochDay); // 序列化为二进制格式
    }

    @Override
    public LocalDate deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        short epochDay = deserializer.readShort(); // 从二进制流反序列化
        return LocalDate.ofEpochDay(epochDay & 0xFFFF); // 返回LocalDate
    }
}
