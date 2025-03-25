/**
 * ClickHouse String数据类型的实现
 * 用于处理ClickHouse中的字符串类型，对应Java中的CharSequence
 * 提供字符串数据的序列化和反序列化功能
 */
package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.misc.BytesCharSeq;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;

/**
 * 字符串数据类型实现
 * 处理ClickHouse中的String类型，对应Java中的CharSequence
 * 字符串在ClickHouse中以二进制格式存储，长度可变
 */
public class DataTypeString implements IDataType<CharSequence> {

    /**
     * 字符串类型创建器
     * 用于创建DataTypeString实例，使用服务器配置中的字符集
     */
    public static DataTypeCreator<CharSequence> CREATOR = (lexer, serverContext) -> new DataTypeString(serverContext.getConfigure().charset());

    /**
     * 用于字符串编解码的字符集
     */
    private final Charset charset;

    /**
     * 创建字符串数据类型
     * 
     * @param charset 用于字符串编解码的字符集
     */
    public DataTypeString(Charset charset) {
        this.charset = charset;
    }

    /**
     * 获取数据类型名称
     * 
     * @return 数据类型名称，即"String"
     */
    @Override
    public String name() {
        return "String";
    }

    /**
     * 获取数据类型默认值
     * 
     * @return 空字符串
     */
    @Override
    public String defaultValue() {
        return "";
    }

    /**
     * 获取数据类型对应的Java类
     * 
     * @return CharSequence.class
     */
    @Override
    public Class<CharSequence> javaType() {
        return CharSequence.class;
    }

    /**
     * 将字符串序列化为二进制格式
     * 如果是BytesCharSeq类型，直接写入其底层字节数组
     * 否则转换为字符串后使用指定字符集编码并写入
     * 
     * @param data 要序列化的字符串
     * @param serializer 二进制序列化器
     * @throws SQLException 如果序列化过程中发生SQL错误
     * @throws IOException 如果序列化过程中发生I/O错误
     */
    @Override
    public void serializeBinary(CharSequence data, BinarySerializer serializer) throws SQLException, IOException {
        if (data instanceof BytesCharSeq) {
            serializer.writeBytesBinary(((BytesCharSeq) data).bytes());
        } else {
            serializer.writeStringBinary(data.toString(), charset);
        }
    }

    @Override
    public void serializeBinary(byte[] bytes, int offset, int length, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeVarInt(length);
        serializer.writeBytes(bytes, offset, length);
    }

    /**
     * 从二进制流反序列化字符串
     * 读取字节数组并使用指定字符集解码为字符串
     * 
     * @param deserializer 二进制反序列化器
     * @return 反序列化后的字符串
     * @throws SQLException 如果反序列化过程中发生SQL错误
     * @throws IOException 如果反序列化过程中发生I/O错误
     */
    @Override
    public String deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        byte[] bs = deserializer.readBytesBinary();
        return new String(bs, charset);
    }
}
