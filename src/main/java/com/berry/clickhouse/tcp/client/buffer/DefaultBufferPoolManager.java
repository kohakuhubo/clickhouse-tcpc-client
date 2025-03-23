package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.data.IDataType;

import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * DefaultBufferPoolManager类实现了BufferPoolManager接口
 * 提供默认的字节缓冲区分配和回收机制
 */
public class DefaultBufferPoolManager implements BufferPoolManager {

    private final int blockSize; // 缓冲区块大小

    /**
     * 构造函数，初始化DefaultBufferPoolManager
     * 
     * @param blockSize 缓冲区块大小
     */
    public DefaultBufferPoolManager(int blockSize) {
        this.blockSize = blockSize; // 设置缓冲区块大小
    }

    @Override
    public ByteBuffer allocate(String colName, IDataType<?> dataType) {
        return ByteBuffer.allocate(this.blockSize); // 分配指定大小的字节缓冲区
    }

    @Override
    public void recycle(ByteBuffer buffer) {
        ((Buffer) buffer).clear(); // 清空缓冲区以便重用
    }
}
