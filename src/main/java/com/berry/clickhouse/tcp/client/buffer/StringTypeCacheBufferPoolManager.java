package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeString;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class StringTypeCacheBufferPoolManager extends DefaultBufferPoolManager {

    private final Queue<ByteBuffer> queue;

    private final int stringBlockSize;

    public StringTypeCacheBufferPoolManager(int blockSize, int stringBlockSize, int cacheLength) {
        super(blockSize);
        this.queue = new ArrayBlockingQueue<>(cacheLength);
        this.stringBlockSize = stringBlockSize;
    }

    @Override
    public ByteBuffer allocate(String colName, IDataType<?> dataType) {

        ByteBuffer newBuffer;
        if (dataType instanceof DataTypeString) {
            newBuffer = queue.poll();
            if (null == newBuffer)
                newBuffer = ByteBuffer.allocate(this.stringBlockSize);
        } else {
            newBuffer = super.allocate(colName, dataType);
        }
        return newBuffer;
    }

    @Override
    public void recycle(ByteBuffer buffer) {
        super.recycle(buffer);
    }
}
