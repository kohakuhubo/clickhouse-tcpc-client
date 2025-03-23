package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BufferPoolManager;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeString;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * StringTypeColumnWriterBufferPoolManager类实现了ColumnWriterBufferPoolManager接口
 * 专门用于管理String类型列的写入缓冲区
 */
public class StringTypeColumnWriterBufferPoolManager implements ColumnWriterBufferPoolManager {

    private final Queue<ColumnWriterBuffer> queue; // 存储String类型列的写入缓冲区队列
    private final int stringSelfBufferSize; // String类型列的自定义缓冲区大小
    private final DefaultColumnWriterBufferPoolManager defaultManager; // 默认缓冲区管理器

    /**
     * 构造函数，初始化StringTypeColumnWriterBufferPoolManager
     * 
     * @param stackSize 缓冲区队列大小
     * @param selfBufferSize 自定义缓冲区大小
     * @param stringStackSize String类型列的缓冲区队列大小
     * @param stringSelfBufferSize String类型列的自定义缓冲区大小
     */
    public StringTypeColumnWriterBufferPoolManager(int stackSize, int selfBufferSize, int stringStackSize, int stringSelfBufferSize) {
        this.defaultManager = new DefaultColumnWriterBufferPoolManager(stackSize, selfBufferSize); // 初始化默认缓冲区管理器
        this.stringSelfBufferSize = stringSelfBufferSize; // 设置String类型列的自定义缓冲区大小
        this.queue = new ArrayBlockingQueue<>(stringStackSize); // 初始化String类型列的缓冲区队列
    }

    @Override
    public ColumnWriterBuffer allocate(IColumn column, BufferPoolManager bufferPoolManager) {
        ColumnWriterBuffer columnWriterBuffer;
        if (column.type() instanceof DataTypeString) {
            columnWriterBuffer = queue.poll(); // 从队列中获取缓冲区
            if (null == columnWriterBuffer) {
                columnWriterBuffer = new ColumnWriterBuffer(this, this.stringSelfBufferSize, // 创建新的缓冲区
                        () -> bufferPoolManager.allocate(column.name(), column.type()),
                        bufferPoolManager::recycle);
            } else {
                columnWriterBuffer.reset(); // 重置缓冲区
            }
        } else {
            columnWriterBuffer = defaultManager.allocate(column, bufferPoolManager); // 使用默认管理器分配缓冲区
        }
        return columnWriterBuffer; // 返回缓冲区
    }

    @Override
    public void recycle(ColumnWriterBuffer columnWriterBuffer) {
        if (columnWriterBuffer.getManager() == this) {
            queue.offer(columnWriterBuffer); // 将缓冲区放回队列
        } else {
            defaultManager.recycle(columnWriterBuffer); // 使用默认管理器回收缓冲区
        }
    }
}
