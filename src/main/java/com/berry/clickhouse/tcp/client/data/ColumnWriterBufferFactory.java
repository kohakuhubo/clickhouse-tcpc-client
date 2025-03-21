package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.settings.ClickHouseConfig;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class ColumnWriterBufferFactory {

    private final Queue<ColumnWriterBuffer> stack;

    private static volatile ColumnWriterBufferFactory INSTANCE;

    private final Queue<ByteBuffer> systemByteBufferQueue;

    private final Queue<ColumnWriterBuffer> systemColumnWriterBufferQueue;

    private final ClickHouseConfig clickHouseConfig;

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
        this.clickHouseConfig = clickHouseConfig;
        this.stack = new ArrayBlockingQueue<>(clickHouseConfig.getSelfColumStackLength());
        this.systemByteBufferQueue = new ArrayBlockingQueue<>(clickHouseConfig.getSystemByteBufferStackLength());
        this.systemColumnWriterBufferQueue = new ArrayBlockingQueue<>(clickHouseConfig.getSystemColumStackLength());
    }

    public ColumnWriterBuffer getBuffer(boolean isUserSystemBuffer) {
        Queue<ColumnWriterBuffer> queue = isUserSystemBuffer ? systemColumnWriterBufferQueue : stack;
        ColumnWriterBuffer pop = queue.poll();
        if (pop == null) {
            int size = isUserSystemBuffer ? this.clickHouseConfig.getSystemByteBufferSize() : this.clickHouseConfig.getSelfByteBufferSize();
            int length = isUserSystemBuffer ? this.clickHouseConfig.getSystemByteBufferLength() : this.clickHouseConfig.getSelfByteBufferLength();
            return new ColumnWriterBuffer(size, length, isUserSystemBuffer ? systemByteBufferQueue : null);
        } else {
            pop.reset();
            return pop;
        }
    }

    public void clearAllBuffers() {
        while (true)  {
            ColumnWriterBuffer pop = stack.poll();
            if (pop == null) {
                break;
            }
        }
    }

    public void recycleBuffer(ColumnWriterBuffer buffer) {
        Queue<ColumnWriterBuffer> stack = buffer.isUseSysColumnWriterBuffer() ? this.systemColumnWriterBufferQueue : this.stack;
        stack.offer(buffer);
    }
}
