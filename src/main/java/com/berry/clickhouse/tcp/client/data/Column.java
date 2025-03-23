package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Column类实现了IColumn接口
 * 表示ClickHouse中的一列数据，包含列名、数据类型和数据值
 */
public class Column extends AbstractColumn {

    /**
     * 构造函数，初始化Column
     * 
     * @param name 列名
     * @param type 数据类型
     * @param nameBytes 列名字节数组
     * @param values 列值数组
     */
    public Column(String name, IDataType<?> type, byte[] nameBytes, Object[] values) {
        super(name, type, nameBytes, values); // 调用父类构造函数
        this.values = values; // 设置列值
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        type().serializeBinary(object, buffer.column); // 将对象序列化为二进制格式
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean now) throws IOException, SQLException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name); // 写入列名
            serializer.writeUTF8StringBinary(type.name()); // 写入数据类型名称
        }

        if (now) {
            buffer.writeTo(serializer); // 立即写入缓冲区
        }
    }

    @Override
    public void recycleColumnWriterBuffer(ColumnWriterBufferFactory factory) {
        if (null != this.buffer) {
            factory.recycleBuffer(buffer); // 回收缓冲区
            this.setColumnWriterBuffer(null); // 清空缓冲区
        }
    }

    @Override
    public void rewind() {
        this.buffer.rewind(); // 重置缓冲区位置
    }
}
