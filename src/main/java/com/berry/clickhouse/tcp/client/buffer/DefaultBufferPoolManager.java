package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.data.IDataType;

import java.nio.Buffer;
import java.nio.ByteBuffer;

public class DefaultBufferPoolManager implements BufferPoolManager {

    private final int blockSize;

    public DefaultBufferPoolManager(int blockSize) {
        this.blockSize = blockSize;
    }

    @Override
    public ByteBuffer allocate(String colName, IDataType<?> dataType) {
        return ByteBuffer.allocate(this.blockSize);
    }

    @Override
    public void recycle(ByteBuffer buffer) {
        ((Buffer) buffer).clear();
    }
}
