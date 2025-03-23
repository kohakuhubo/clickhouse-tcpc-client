package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeMap;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ColumnMap类实现了IColumn接口
 * 表示ClickHouse中的映射列，包含键值对
 */
public class ColumnMap extends AbstractColumn {
    private final IColumn[] columnDataArray; // 存储键值对的列数组
    private final List<Long> offsets; // 存储偏移量

    /**
     * 构造函数，初始化ColumnMap
     * 
     * @param name 列名
     * @param type 数据类型
     * @param nameBytes 列名字节数组
     * @param values 列值数组
     */
    public ColumnMap(String name, DataTypeMap type, byte[] nameBytes, Object[] values) {
        super(name, type, nameBytes, values); // 调用父类构造函数
        offsets = new ArrayList<>(); // 初始化偏移量列表
        IDataType<?>[] types = type.getNestedTypes(); // 获取嵌套数据类型
        columnDataArray = new IColumn[types.length]; // 创建列数组
        for (int i = 0; i < types.length; i++) {
            columnDataArray[i] = ColumnFactory.createColumn(null, types[i], nameBytes, null); // 创建列
        }
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        if (object instanceof Map) {
            Map<?, ?> dataMap = (Map<?, ?>) object; // 获取映射数据
            offsets.add(offsets.isEmpty() ? dataMap.size() : offsets.get((offsets.size() - 1)) + dataMap.size()); // 更新偏移量

            for (Object key : dataMap.keySet()) {
                columnDataArray[0].write(key); // 写入键
            }
            for (Object value : dataMap.values()) {
                columnDataArray[1].write(value); // 写入值
            }
        }
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean now) throws IOException, SQLException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name); // 写入列名
            serializer.writeUTF8StringBinary(type.name()); // 写入数据类型名称
        }

        flushOffsets(serializer); // 写入偏移量

        for (IColumn data : columnDataArray) {
            data.flushToSerializer(serializer, true); // 写入列数据
        }

        if (now) {
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
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer, ColumnWriterBufferFactory factory) {
        super.setColumnWriterBuffer(buffer); // 设置列的写入缓冲区

        for (IColumn data : columnDataArray) {
            data.setColumnWriterBuffer(factory.getBuffer(data)); // 设置嵌套列的写入缓冲区
        }
    }

    @Override
    public void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory) {
        if (null != this.buffer) {
            factory.recycleBuffer(this.buffer); // 回收列的写入缓冲区
            super.setColumnWriterBuffer(null); // 清空列的写入缓冲区
        }
        for (IColumn data : columnDataArray) {
            data.recycleColumnWriterBuffer(factory); // 回收嵌套列的写入缓冲区
        }
    }

    @Override
    public void rewind() {
        this.buffer.rewind(); // 重置列的读取位置
        for (IColumn data : columnDataArray) {
            data.rewind(); // 重置嵌套列的读取位置
        }
    }

    @Override
    public void clear() {
        super.clear(); // 清空列
        offsets.clear(); // 清空偏移量列表
        for (IColumn data : columnDataArray) {
            data.clear(); // 清空嵌套列
        }
    }
}
