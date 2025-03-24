/**
 * ClickHouse Int32数据类型的实现
 * 对应ClickHouse中的Int32类型，Java中的Integer类型
 * 表示带符号的32位整数，范围为-2,147,483,648到2,147,483,647
 */
package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Int32数据类型实现
 * 实现ClickHouse中Int32类型的序列化和反序列化
 */
public class DataTypeInt32 implements BaseDataTypeInt32<Integer> {

    /**
     * 获取数据类型名称
     * 
     * @return 返回"Int32"
     */
    @Override
    public String name() {
        return "Int32";
    }

    /**
     * 获取数据类型默认值
     * 
     * @return 默认值0
     */
    @Override
    public Integer defaultValue() {
        return 0;
    }

    /**
     * 获取数据类型对应的Java类
     * 
     * @return Integer.class
     */
    @Override
    public Class<Integer> javaType() {
        return Integer.class;
    }

    /**
     * 将Integer值序列化为二进制格式
     * 
     * @param data 要序列化的整数
     * @param serializer 二进制序列化器
     * @throws SQLException 如果序列化过程中发生SQL错误
     * @throws IOException 如果序列化过程中发生I/O错误
     */
    @Override
    public void serializeBinary(Integer data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeInt(data);
    }

    /**
     * 从二进制流反序列化Integer值
     * 
     * @param deserializer 二进制反序列化器
     * @return 反序列化后的整数
     * @throws SQLException 如果反序列化过程中发生SQL错误
     * @throws IOException 如果反序列化过程中发生I/O错误
     */
    @Override
    public Integer deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readInt();
    }

    /**
     * 判断数据类型是否有符号
     * 
     * @return 返回true，表示Int32是有符号的
     */
    @Override
    public boolean isSigned() {
        return true;
    }
}
