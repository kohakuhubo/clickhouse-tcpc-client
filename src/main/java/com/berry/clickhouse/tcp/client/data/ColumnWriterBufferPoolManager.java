package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BufferPoolManager;

public interface ColumnWriterBufferPoolManager {

    ColumnWriterBuffer allocate(IColumn column, BufferPoolManager bufferPoolManager);

    void recycle(ColumnWriterBuffer columnWriterBuffer);

}
