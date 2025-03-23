package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * IColumn接口定义了列的基本操作
 * 所有列类型都必须实现该接口
 */
public interface IColumn {

    /**
     * 检查列是否被导出
     * 
     * @return 如果列被导出则返回true，否则返回false
     */
    boolean isExported();

    /**
     * 获取列名
     * 
     * @return 列名
     */
    String name();

    /**
     * 获取列的数据类型
     * 
     * @return 数据类型
     */
    IDataType<?> type();

    /**
     * 获取指定索引的值
     * 
     * @param idx 索引
     * @return 值
     */
    Object value(int idx);

    /**
     * 设置列的值
     * 
     * @param values 值数组
     */
    void setValues(Object[] values);

    /**
     * 将对象写入列
     * 
     * @param object 要写入的对象
     * @throws IOException 如果写入过程中发生I/O错误
     * @throws SQLException 如果写入过程中发生SQL错误
     */
    void write(Object object) throws IOException, SQLException;

    /**
     * 从二进制反序列化器读取数据
     * 
     * @param rows 行数
     * @param binaryDeserializer 二进制反序列化器
     * @throws IOException 如果读取过程中发生I/O错误
     * @throws SQLException 如果读取过程中发生SQL错误
     */
    void read(int rows, BinaryDeserializer binaryDeserializer) throws IOException, SQLException;

    /**
     * 将列数据写入二进制序列化器
     * 
     * @param serializer 二进制序列化器
     * @param now 是否立即写入
     * @throws IOException 如果写入过程中发生I/O错误
     * @throws SQLException 如果写入过程中发生SQL错误
     */
    void flushToSerializer(BinarySerializer serializer, boolean now) throws IOException, SQLException;

    /**
     * 清空列
     */
    void clear();

    /**
     * 设置列的写入缓冲区
     * 
     * @param buffer 写入缓冲区
     */
    void setColumnWriterBuffer(ColumnWriterBuffer buffer);

    /**
     * 获取列的写入缓冲区
     * 
     * @return 写入缓冲区
     */
    ColumnWriterBuffer getColumnWriterBuffer();

    /**
     * 获取行数
     * 
     * @return 行数
     */
    int rowCnt();

    /**
     * 增加行数
     * 
     * @return 当前行数
     */
    int addRowCnt();

    /**
     * 增加指定数量的行数
     * 
     * @param count 增加的行数
     * @return 当前行数
     */
    int addRowCnt(int count);

    /**
     * 重置列的读取位置
     */
    void rewind();

    /**
     * 获取列的所有值
     * 
     * @return 值数组
     */
    Object[] values();

    default void setColumnWriterBuffer(ColumnWriterBuffer buffer, ColumnWriterBufferFactory factory) {
        setColumnWriterBuffer(buffer);
    }

    void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory);
}


