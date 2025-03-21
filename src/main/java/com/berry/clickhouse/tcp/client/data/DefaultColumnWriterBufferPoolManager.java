package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BufferPoolManager;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class DefaultColumnWriterBufferPoolManager implements ColumnWriterBufferPoolManager {

    private final int selfBufferSize;

    private final Queue<ColumnWriterBuffer> stack;

    public DefaultColumnWriterBufferPoolManager(int stackSize, int selfBufferSize) {
        this.selfBufferSize = selfBufferSize;
        this.stack = new ArrayBlockingQueue<>(stackSize);
    }

    @Override
    public ColumnWriterBuffer allocate(IColumn column, BufferPoolManager bufferPoolManager) {
        ColumnWriterBuffer pop = stack.poll();
        if (null == pop) {
            pop = new ColumnWriterBuffer(this, this.selfBufferSize, () -> bufferPoolManager.allocate(column.name(), column.type()),
                    bufferPoolManager::recycle);
        } else {
            pop.reset();
        }
        return pop;
    }

    @Override
    public void recycle(ColumnWriterBuffer columnWriterBuffer) {
        stack.offer(columnWriterBuffer);
    }
}
