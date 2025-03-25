package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeArray;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseArray;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ColumnArray类实现了IColumn接口
 * 表示ClickHouse中的数组列，包含数组的元素
 */
public class ColumnArray extends AbstractColumn {

    private final List<Long> offsets; // 存储数组的偏移量
    private final IColumn data; // 存储数组元素的列

    /**
     * 构造函数，初始化ColumnArray
     * 
     * @param name 列名
     * @param type 数据类型
     * @param nameBytes 列名字节数组
     * @param values 列值数组
     */
    public ColumnArray(String name, DataTypeArray type, byte[] nameBytes, Object[] values) {
        super(name, type, nameBytes, values); // 调用父类构造函数
        offsets = new ArrayList<>(); // 初始化偏移量列表
        data = ColumnFactory.createColumn(null, type.getElemDataType(), nameBytes, null); // 创建元素列
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        int len = Array.getLength(object);
        offsets.add(offsets.isEmpty() ? len : offsets.get((offsets.size() - 1)) + len); // 更新偏移量
        for (int i = 0; i < len; i++) {
            data.write(Array.get(object, i));// 写入数组元素
        }
        addRowCnt();
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean immediate) throws SQLException, IOException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name); // 写入列名
            serializer.writeUTF8StringBinary(type.name()); // 写入数据类型名称
        }

        flushOffsets(serializer); // 写入偏移量
        data.flushToSerializer(serializer, false); // 写入元素列

        if (immediate) {
            buffer.writeTo(serializer); // 立即写入缓冲区
        }
    }

    /**
     * 写入偏移量到序列化器
     * 
     * @param serializer 二进制序列化器
     * @throws IOException IO异常
     */
    public void flushOffsets(BinarySerializer serializer) throws IOException {
        for (long offsetList : offsets) {
            serializer.writeLong(offsetList); // 写入偏移量
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer); // 设置列的写入缓冲区
        data.setColumnWriterBuffer(buffer); // 设置元素列的写入缓冲区
    }

    @Override
    public void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory) {
        this.setColumnWriterBuffer(null); // 清空列的写入缓冲区
        data.recycleColumnWriterBuffer(factory); // 回收元素列的写入缓冲区
    }

    @Override
    public void rewind() {
        this.data.rewind(); // 重置元素列的读取位置
    }

    @Override
    public void clear() {
        super.clear(); // 清空列
        offsets.clear(); // 清空偏移量列表
        data.clear(); // 清空元素列
    }
}
