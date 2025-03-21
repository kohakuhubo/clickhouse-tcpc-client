package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BufferPoolManager;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeString;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class StringTypeColumnWriterBufferPoolManager implements ColumnWriterBufferPoolManager {

    private final Queue<ColumnWriterBuffer> queue;

    private final int stringSelfBufferSize;

    private final DefaultColumnWriterBufferPoolManager defaultManager;

    public StringTypeColumnWriterBufferPoolManager(int stackSize, int selfBufferSize, int stringStackSize, int stringSelfBufferSize) {
        this.defaultManager = new DefaultColumnWriterBufferPoolManager(stackSize, selfBufferSize);
        this.stringSelfBufferSize = stringSelfBufferSize;
        this.queue = new ArrayBlockingQueue<>(stringStackSize);
    }

    @Override
    public ColumnWriterBuffer allocate(IColumn column, BufferPoolManager bufferPoolManager) {

        ColumnWriterBuffer columnWriterBuffer;
        if (column.type() instanceof DataTypeString) {
            columnWriterBuffer = queue.poll();
            if (null == columnWriterBuffer) {
                columnWriterBuffer = new ColumnWriterBuffer(this, this.stringSelfBufferSize,
                        () -> bufferPoolManager.allocate(column.name(), column.type()),
                        bufferPoolManager::recycle);
            } else {
                columnWriterBuffer.reset();
            }
        } else {
            columnWriterBuffer = defaultManager.allocate(column, bufferPoolManager);
        }
        return columnWriterBuffer;
    }

    @Override
    public void recycle(ColumnWriterBuffer columnWriterBuffer) {
        if (columnWriterBuffer.getManager() == this) {
            queue.offer(columnWriterBuffer);
        } else {
            defaultManager.recycle(columnWriterBuffer);
        }
    }
}
