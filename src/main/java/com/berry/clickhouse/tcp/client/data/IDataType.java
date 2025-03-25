/**
 * ClickHouse数据类型接口
 * 定义了所有ClickHouse数据类型必须实现的方法
 * 提供了数据序列化和反序列化的通用接口
 */
package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BuffedReadWriter;
import com.berry.clickhouse.tcp.client.exception.ClickHouseSQLException;
import com.berry.clickhouse.tcp.client.exception.InvalidOperationException;
import com.berry.clickhouse.tcp.client.exception.NoDefaultValueException;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * IDataType接口定义了ClickHouse数据类型的基本操作
 * 包括序列化、反序列化和数据类型信息的获取
 *
 * @param <CK> 数据类型对应的Java类型
 */
public interface IDataType<CK> {

    /**
     * 获取数据类型名称
     *
     * @return 数据类型名称（如Int32、String等）
     */
    String name();

    default int byteSize() {
        return -1;
    }

    /**
     * 获取数据类型的默认值
     *
     * @return 数据类型默认值
     * @throws NoDefaultValueException 如果数据类型没有默认值
     */
    default CK defaultValue() {
        throw new NoDefaultValueException("Column[" + name() + "] doesn't has default value");
    }

    /**
     * 获取数据类型对应的Java类
     *
     * @return 数据类型对应的Java类
     */
    Class<CK> javaType();

    /**
     * 判断数据类型是否可为空
     *
     * @return 如果数据类型可为空则返回true，否则返回false
     */
    default boolean nullable() {
        return false;
    }

    /**
     * 判断数据类型是否有符号
     *
     * @return 如果数据类型有符号则返回true，否则返回false
     */
    default boolean isSigned() {
        return false;
    }

    /**
     * 将数据序列化为二进制格式
     *
     * @param data       要序列化的数据
     * @param serializer 二进制序列化器
     * @throws SQLException 如果序列化过程中发生SQL错误
     * @throws IOException  如果序列化过程中发生I/O错误
     */
    void serializeBinary(CK data, BinarySerializer serializer) throws SQLException, IOException;

    /**
     * 将数据序列化为二进制格式
     *
     * @param byt
     * @param serializer
     * @throws SQLException
     * @throws IOException
     */
    default void serializeByteBinary(byte byt, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeByte(byt);
    }

    /**
     * 将数据序列化为二进制格式
     *
     * @param bytes      字节数组
     * @param offset     偏移量
     * @param length     长度
     * @param serializer 序列化器
     * @throws SQLException
     * @throws IOException
     */
    default void serializeBinary(byte[] bytes, int offset, int length, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeBytes(bytes, offset, length);
    }

    /**
     * 将数据序列化为整数二进制格式，默认字节数组为小端序
     *
     * @param bytes      字节数组
     * @param offset     偏移量
     * @param length     长度
     * @param serializer 序列化器
     * @throws SQLException
     * @throws IOException
     */
    default void serializeIntBinary(byte[] bytes, int offset, int length, BinarySerializer serializer) throws SQLException, IOException {
        serializeIntBinary(bytes, offset, length, true, serializer);
    }

    /**
     * 将数据序列化为整数二进制格式
     *
     * @param bytes          字节数组
     * @param offset         偏移量
     * @param length         长度
     * @param isLittleEndian 字节数组是否是小端序
     * @param serializer     序列化器
     * @throws SQLException
     * @throws IOException
     */
    default void serializeIntBinary(byte[] bytes, int offset, int length, boolean isLittleEndian, BinarySerializer serializer) throws SQLException, IOException {
        int byteSize = byteSize();
        if (byteSize <= 0) {
            throw new SQLException("This data type[" + name() + "] does not support writing integer byte arrays. ");
        }
        if (isLittleEndian) {
            if (length >= byteSize) {
                serializer.writeBytes(bytes, offset, byteSize);
            } else {
                serializer.writeBytes(bytes, offset, length);
                int zSize = (byteSize - length);
                if (isSigned() && (bytes[offset + length - 1] & 0x80) != 0) {
                    for (int i = 0; i < zSize; i++) {
                        serializer.writeByte((byte) 0xFF);
                    }
                } else {
                    for (int i = 0; i < zSize; i++) {
                        serializer.writeByte((byte) 0);
                    }
                }
            }
        } else {
            if (length >= byteSize) {
                for (int i = (offset + byteSize - 1); i >= 0; i--) {
                    serializer.writeByte(bytes[i]);
                }
            } else {
                for (int i = (offset + length - 1); i >= 0; i--) {
                    serializer.writeByte(bytes[i]);
                }
                int zSize = (byteSize - length);
                if (isSigned() && (bytes[offset] & 0x80) != 0) {
                    for (int i = 0; i < zSize; i++) {
                        serializer.writeByte((byte) 0xFF);
                    }
                } else {
                    for (int i = 0; i < zSize; i++) {
                        serializer.writeByte((byte) 0);
                    }
                }
            }
        }
    }

    /**
     * 批量将数据序列化为二进制格式
     *
     * @param data       要序列化的数据数组
     * @param serializer 二进制序列化器
     * @throws SQLException 如果序列化过程中发生SQL错误
     * @throws IOException  如果序列化过程中发生I/O错误
     */
    default void serializeBinaryBulk(CK[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (CK d : data) {
            serializeBinary(d, serializer);
        }
    }

    /**
     * 从二进制流反序列化数据
     *
     * @param deserializer 二进制反序列化器
     * @return 反序列化后的数据
     * @throws SQLException 如果反序列化过程中发生SQL错误
     * @throws IOException  如果反序列化过程中发生I/O错误
     */
    CK deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException;

    /**
     * 批量从二进制流反序列化数据
     *
     * @param rows         行数
     * @param deserializer 二进制反序列化器
     * @return 反序列化后的数据数组
     * @throws SQLException 如果反序列化过程中发生SQL错误
     * @throws IOException  如果反序列化过程中发生I/O错误
     */
    default Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[] data = new Object[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = this.deserializeBinary(deserializer);
        }
        return data;
    }

    /**
     * 批量从二进制流反序列化数据到缓冲区
     *
     * @param rows             行数
     * @param buffedReadWriter 缓冲读写器
     * @param deserializer     二进制反序列化器
     * @throws SQLException 如果反序列化过程中发生SQL错误
     * @throws IOException  如果反序列化过程中发生I/O错误
     */
    default void deserializeBinaryBulk(int rows, BuffedReadWriter buffedReadWriter, BinaryDeserializer deserializer) throws SQLException, IOException {
        throw new IOException();
    }
}
