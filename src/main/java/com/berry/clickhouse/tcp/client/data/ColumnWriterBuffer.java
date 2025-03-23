package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BufferPoolManager;
import com.berry.clickhouse.tcp.client.buffer.ByteArrayWriter;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * ColumnWriterBuffer类用于管理列的写入缓冲区
 * 提供写入和序列化功能
 */
public class ColumnWriterBuffer {

    private final ByteArrayWriter columnWriter; // 列的字节数组写入器

    public final BinarySerializer column; // 列的二进制序列化器

    public final ColumnWriterBufferPoolManager manager; // 列写入缓冲区管理器

    /**
     * 构造函数，初始化ColumnWriterBuffer
     * 
     * @param manager 列写入缓冲区管理器
     * @param length 缓冲区长度
     * @param allocateBuffer 分配缓冲区的供应商
     * @param recycleBuffer 回收缓冲区的消费者
     */
    public ColumnWriterBuffer(ColumnWriterBufferPoolManager manager, int length, Supplier<ByteBuffer> allocateBuffer, Consumer<ByteBuffer> recycleBuffer) {
        this.columnWriter = new ByteArrayWriter(length, allocateBuffer, recycleBuffer); // 创建字节数组写入器
        this.column = new BinarySerializer(columnWriter, false); // 创建二进制序列化器
        this.manager = manager; // 设置列写入缓冲区管理器
    }

    /**
     * 将数据写入二进制序列化器
     * 
     * @param serializer 二进制序列化器
     * @throws IOException 如果写入过程中发生I/O错误
     */
    public void writeTo(BinarySerializer serializer) throws IOException {
        for (ByteBuffer buffer : columnWriter.getBufferList()) {
            serializer.writeBytes(buffer.array(), buffer.arrayOffset(), buffer.position()); // 写入字节数组
            buffer.limit(buffer.position()); // 设置缓冲区限制
        }
    }

    /**
     * 重置写入缓冲区
     */
    public void reset() {
        columnWriter.reset(); // 重置字节数组写入器
    }

    /**
     * 清空写入缓冲区
     */
    public void clear() {
        columnWriter.clear(); // 清空字节数组写入器
    }

    /**
     * 重置读取位置
     */
    public void rewind() {
        columnWriter.rewind(); // 重置字节数组写入器的读取位置
    }

    /**
     * 获取列写入缓冲区管理器
     * 
     * @return 列写入缓冲区管理器
     */
    public ColumnWriterBufferPoolManager getManager() {
        return manager; // 返回列写入缓冲区管理器
    }
}
