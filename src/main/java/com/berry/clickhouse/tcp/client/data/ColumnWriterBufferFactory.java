package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BufferPoolManager;
import com.berry.clickhouse.tcp.client.settings.ClickHouseConfig;

/**
 * ColumnWriterBufferFactory类用于创建ColumnWriterBuffer实例
 * 提供缓冲区的分配和回收功能
 */
public class ColumnWriterBufferFactory {

    private static volatile ColumnWriterBufferFactory INSTANCE; // 单例实例

    private final ColumnWriterBufferPoolManager columnWriterBufferPoolManager; // 列写入缓冲区管理器

    private final BufferPoolManager bufferPoolManager; // 缓冲池管理器

    /**
     * 获取ColumnWriterBufferFactory的单例实例
     * 
     * @param clickHouseConfig ClickHouse配置
     * @return ColumnWriterBufferFactory实例
     */
    public static ColumnWriterBufferFactory getInstance(ClickHouseConfig clickHouseConfig) {
        if (null == INSTANCE) {
            synchronized (ColumnWriterBufferFactory.class) {
                if (null == INSTANCE) {
                    INSTANCE = new ColumnWriterBufferFactory(clickHouseConfig); // 创建新的实例
                }
            }
        }
        return INSTANCE; // 返回单例实例
    }

    /**
     * 构造函数，初始化ColumnWriterBufferFactory
     * 
     * @param clickHouseConfig ClickHouse配置
     */
    private ColumnWriterBufferFactory(ClickHouseConfig clickHouseConfig) {
        this.columnWriterBufferPoolManager = clickHouseConfig.getColumnWriterBufferPoolManager(); // 获取列写入缓冲区管理器
        this.bufferPoolManager = clickHouseConfig.getBufferPoolManager(); // 获取缓冲池管理器
    }

    /**
     * 获取列的写入缓冲区
     * 
     * @param column 列对象
     * @return ColumnWriterBuffer实例
     */
    public ColumnWriterBuffer getBuffer(IColumn column) {
        return this.columnWriterBufferPoolManager.allocate(column, this.bufferPoolManager); // 分配列的写入缓冲区
    }

    /**
     * 回收列的写入缓冲区
     * 
     * @param buffer 列的写入缓冲区
     */
    public void recycleBuffer(ColumnWriterBuffer buffer) {
        this.columnWriterBufferPoolManager.recycle(buffer); // 回收列的写入缓冲区
    }
}
