package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeString;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * StringTypeCacheBufferPoolManager类扩展了DefaultBufferPoolManager
 * 提供针对字符串类型的缓冲区缓存管理
 */
public class StringTypeCacheBufferPoolManager extends DefaultBufferPoolManager {

    private final Queue<ByteBuffer> queue; // 字节缓冲区队列
    private final int stringBlockSize; // 字符串块大小

    /**
     * 构造函数，初始化StringTypeCacheBufferPoolManager
     * 
     * @param blockSize 缓冲区块大小
     * @param stringBlockSize 字符串块大小
     * @param cacheLength 缓存长度
     */
    public StringTypeCacheBufferPoolManager(int blockSize, int stringBlockSize, int cacheLength) {
        super(blockSize); // 调用父类构造函数
        this.queue = new ArrayBlockingQueue<>(cacheLength); // 初始化字节缓冲区队列
        this.stringBlockSize = stringBlockSize; // 设置字符串块大小
    }

    @Override
    public ByteBuffer allocate(String colName, IDataType<?> dataType) {
        ByteBuffer newBuffer;
        if (dataType instanceof DataTypeString) {
            newBuffer = queue.poll(); // 从队列中获取缓冲区
            if (null == newBuffer) {
                newBuffer = ByteBuffer.allocate(this.stringBlockSize); // 分配新的字符串缓冲区
            }
        } else {
            newBuffer = super.allocate(colName, dataType); // 调用父类的分配方法
        }
        return newBuffer; // 返回分配的缓冲区
    }

    @Override
    public void recycle(ByteBuffer buffer) {
        super.recycle(buffer); // 调用父类的回收方法
    }
}
