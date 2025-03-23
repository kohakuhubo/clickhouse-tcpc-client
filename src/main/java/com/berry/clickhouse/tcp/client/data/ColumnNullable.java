package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeNullable;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * ColumnNullable类实现了IColumn接口
 * 表示ClickHouse中的可空列，包含可空值
 */
public class ColumnNullable extends AbstractColumn {

    private final List<Byte> nullableSign; // 存储可空标志
    private final IColumn data; // 存储实际数据的列

    /**
     * 构造函数，初始化ColumnNullable
     * 
     * @param name 列名
     * @param type 数据类型
     * @param nameBytes 列名字节数组
     * @param values 列值数组
     */
    public ColumnNullable(String name, DataTypeNullable type, byte[] nameBytes, Object[] values) {
        super(name, type, nameBytes, values); // 调用父类构造函数
        nullableSign = new ArrayList<>(); // 初始化可空标志列表
        data = ColumnFactory.createColumn(null, type.getNestedDataType(), nameBytes, null); // 创建实际数据列
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        if (object == null) {
            nullableSign.add((byte) 1); // 添加可空标志
            data.write(type.defaultValue()); // 写入默认值
        } else {
            nullableSign.add((byte) 0); // 添加非空标志
            data.write(object); // 写入实际值
        }
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean immediate) throws IOException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name); // 写入列名
            serializer.writeUTF8StringBinary(type.name()); // 写入数据类型名称
        }

        for (byte sign : nullableSign) {
            serializer.writeByte(sign); // 写入可空标志
        }

        if (immediate) {
            buffer.writeTo(serializer); // 立即写入缓冲区
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer); // 设置列的写入缓冲区
        data.setColumnWriterBuffer(buffer); // 设置实际数据列的写入缓冲区
    }

    @Override
    public void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory) {
        super.setColumnWriterBuffer(null); // 清空列的写入缓冲区
        this.data.recycleColumnWriterBuffer(factory); // 回收实际数据列的写入缓冲区
    }

    @Override
    public void rewind() {
        this.data.rewind(); // 重置实际数据列的读取位置
    }
}
