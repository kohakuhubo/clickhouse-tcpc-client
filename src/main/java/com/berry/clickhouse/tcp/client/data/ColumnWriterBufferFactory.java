package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BufferPoolManager;
import com.berry.clickhouse.tcp.client.settings.ClickHouseConfig;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Supplier;

public class ColumnWriterBufferFactory {

    private static volatile ColumnWriterBufferFactory INSTANCE;

    private final ColumnWriterBufferPoolManager columnWriterBufferPoolManager;

    private final BufferPoolManager bufferPoolManager;

    public static ColumnWriterBufferFactory getInstance(ClickHouseConfig clickHouseConfig) {
        if (null == INSTANCE) {
            synchronized (ColumnWriterBufferFactory.class) {
                if (null == INSTANCE) {
                    INSTANCE = new ColumnWriterBufferFactory(clickHouseConfig);
                }
            }
        }
        return INSTANCE;
    }

    private ColumnWriterBufferFactory(ClickHouseConfig clickHouseConfig) {
        this.columnWriterBufferPoolManager = clickHouseConfig.getColumnWriterBufferPoolManager();
        this.bufferPoolManager = clickHouseConfig.getBufferPoolManager();
    }

    public ColumnWriterBuffer getBuffer(IColumn column) {
        return this.columnWriterBufferPoolManager.allocate(column, this.bufferPoolManager);
    }

    public void recycleBuffer(ColumnWriterBuffer buffer) {
        this.columnWriterBufferPoolManager.recycle(buffer);
    }
}
