package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.data.IDataType;

import java.nio.ByteBuffer;

public interface BufferPoolManager {

    ByteBuffer allocate(String colName, IDataType<?> dataType);

    void recycle(ByteBuffer buffer);

}
