package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BufferPoolManager;

/**
 * ColumnWriterBufferPoolManager接口定义了列写入缓冲区管理器的基本操作
 * 包括分配和回收列写入缓冲区
 */
public interface ColumnWriterBufferPoolManager {

    /**
     * 分配列的写入缓冲区
     * 
     * @param column 列对象
     * @param bufferPoolManager 缓冲池管理器
     * @return 分配的ColumnWriterBuffer实例
     */
    ColumnWriterBuffer allocate(IColumn column, BufferPoolManager bufferPoolManager);

    /**
     * 回收列的写入缓冲区
     * 
     * @param columnWriterBuffer 列的写入缓冲区
     */
    void recycle(ColumnWriterBuffer columnWriterBuffer);

}
