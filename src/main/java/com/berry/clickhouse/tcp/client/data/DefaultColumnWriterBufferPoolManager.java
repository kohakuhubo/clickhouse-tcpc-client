package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BufferPoolManager;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * DefaultColumnWriterBufferPoolManager类实现了ColumnWriterBufferPoolManager接口
 * 提供默认的列写入缓冲区管理器，支持缓冲区的分配和回收
 */
public class DefaultColumnWriterBufferPoolManager implements ColumnWriterBufferPoolManager {

    private final int selfBufferSize; // 自定义缓冲区大小
    private final Queue<ColumnWriterBuffer> stack; // 缓冲区队列

    /**
     * 构造函数，初始化DefaultColumnWriterBufferPoolManager
     * 
     * @param stackSize 缓冲区队列大小
     * @param selfBufferSize 自定义缓冲区大小
     */
    public DefaultColumnWriterBufferPoolManager(int stackSize, int selfBufferSize) {
        this.selfBufferSize = selfBufferSize; // 设置自定义缓冲区大小
        this.stack = new ArrayBlockingQueue<>(stackSize); // 初始化缓冲区队列
    }

    @Override
    public ColumnWriterBuffer allocate(IColumn column, BufferPoolManager bufferPoolManager) {
        ColumnWriterBuffer pop = stack.poll(); // 从队列中获取缓冲区
        if (null == pop) {
            pop = new ColumnWriterBuffer(this, this.selfBufferSize, // 创建新的缓冲区
                    () -> bufferPoolManager.allocate(column.name(), column.type()),
                    bufferPoolManager::recycle);
        } else {
            pop.reset(); // 重置缓冲区
        }
        return pop; // 返回缓冲区
    }

    @Override
    public void recycle(ColumnWriterBuffer columnWriterBuffer) {
        stack.offer(columnWriterBuffer); // 将缓冲区放回队列
    }
}
