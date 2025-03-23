package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeTuple;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseStruct;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * ColumnTuple类实现了IColumn接口
 * 表示ClickHouse中的元组列，包含多个字段
 */
public class ColumnTuple extends AbstractColumn {

    private final IColumn[] columnDataArray; // 存储元组字段的列数组

    /**
     * 构造函数，初始化ColumnTuple
     * 
     * @param name 列名
     * @param type 数据类型
     * @param nameBytes 列名字节数组
     * @param values 列值数组
     */
    public ColumnTuple(String name, DataTypeTuple type, byte[] nameBytes, Object[] values) {
        super(name, type, nameBytes, values); // 调用父类构造函数
        IDataType<?>[] types = type.getNestedTypes(); // 获取嵌套数据类型
        columnDataArray = new IColumn[types.length]; // 创建列数组
        for (int i = 0; i < types.length; i++) {
            columnDataArray[i] = ColumnFactory.createColumn(null, types[i], nameBytes, null); // 创建列
        }
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        ClickHouseStruct tuple = (ClickHouseStruct) object; // 将对象转换为ClickHouseStruct
        for (int i = 0; i < columnDataArray.length; i++) {
            columnDataArray[i].write(tuple.getAttributes()[i]); // 写入元组字段
        }
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean now) throws SQLException, IOException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name); // 写入列名
            serializer.writeUTF8StringBinary(type.name()); // 写入数据类型名称
        }

        for (IColumn data : columnDataArray) {
            data.flushToSerializer(serializer, true); // 写入元组字段
        }

        if (now) {
            buffer.writeTo(serializer); // 立即写入缓冲区
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer, ColumnWriterBufferFactory factory) {
        super.setColumnWriterBuffer(buffer); // 设置列的写入缓冲区

        for (IColumn data : columnDataArray) {
            data.setColumnWriterBuffer(factory.getBuffer(data)); // 设置嵌套列的写入缓冲区
        }
    }

    @Override
    public void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory) {
        if (null != buffer) {
            factory.recycleBuffer(buffer); // 回收列的写入缓冲区
            super.setColumnWriterBuffer(null); // 清空列的写入缓冲区
        }

        for (IColumn data : columnDataArray) {
            data.recycleColumnWriterBuffer(factory); // 回收嵌套列的写入缓冲区
        }
    }

    @Override
    public void rewind() {
        this.buffer.rewind(); // 重置列的读取位置
        for (IColumn column : columnDataArray) {
            column.rewind(); // 重置嵌套列的读取位置
        }
    }

    @Override
    public void clear() {
        super.clear(); // 清空列
        if (null != this.buffer) {
            this.buffer.clear(); // 清空缓冲区
        }
        for (IColumn column : columnDataArray) {
            column.clear(); // 清空嵌套列
        }
    }
}
