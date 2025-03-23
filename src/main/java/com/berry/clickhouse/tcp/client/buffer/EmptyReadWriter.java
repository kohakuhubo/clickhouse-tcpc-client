package com.berry.clickhouse.tcp.client.buffer;

import java.io.IOException;

/**
 * EmptyReadWriter类实现了BuffedReadWriter接口
 * 提供空的读写操作，通常用于占位或默认实现
 */
public class EmptyReadWriter implements BuffedReadWriter {

    public static final EmptyReadWriter DEFAULT = new EmptyReadWriter(); // 默认实例

    @Override
    public void writeBinary(byte byt) throws IOException {
        // 空实现
    }

    @Override
    public void writeBinary(byte[] bytes) throws IOException {
        // 空实现
    }

    @Override
    public void writeBinary(byte[] bytes, int offset, int length) throws IOException {
        // 空实现
    }

    @Override
    public void writeBinaryReverse(byte[] bytes) throws IOException {
        // 空实现
    }

    @Override
    public void writeBinaryReverse(byte[] bytes, int offset, int length) throws IOException {
        // 空实现
    }

    @Override
    public void flushToTarget(boolean force) throws IOException {
        // 空实现
    }

    @Override
    public int readBinary() throws IOException {
        return 0; // 返回0表示没有数据
    }

    @Override
    public int readBinary(byte[] bytes) throws IOException {
        return 0; // 返回0表示没有数据
    }

    @Override
    public void rewind() {
        // 空实现
    }

    @Override
    public void clear() {
        // 空实现
    }
}
