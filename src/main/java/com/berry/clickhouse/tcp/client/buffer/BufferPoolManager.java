package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.data.IDataType;

import java.nio.ByteBuffer;

/**
 * BufferPoolManager接口定义了缓冲池管理器的基本操作
 * 包括分配和回收字节缓冲区
 */
public interface BufferPoolManager {

    /**
     * 分配一个字节缓冲区
     * 
     * @param colName 列名
     * @param dataType 数据类型
     * @return 分配的字节缓冲区
     */
    ByteBuffer allocate(String colName, IDataType<?> dataType);

    /**
     * 回收一个字节缓冲区
     * 
     * @param buffer 要回收的字节缓冲区
     */
    void recycle(ByteBuffer buffer);

}
