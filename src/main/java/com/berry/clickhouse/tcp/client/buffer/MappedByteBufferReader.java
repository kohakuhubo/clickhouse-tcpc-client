package com.berry.clickhouse.tcp.client.buffer;

import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;

/**
 * MappedByteBufferReader类实现了BuffedReader接口
 * 用于从映射的字节缓冲区中读取数据
 */
public class MappedByteBufferReader implements BuffedReader {

    private static final int MAX_BUFFER_LENGTH = 2 * 1024 * 1024; // 最大缓冲区长度

    private static int capacity; // 缓冲区容量

    private int leftLength; // 剩余长度

    private final byte[] buf; // 缓冲区字节数组

    private MappedByteBuffer buffer; // 映射的字节缓冲区

    private int limit; // 当前限制
    private int position; // 当前读取位置

    /**
     * 构造函数，初始化MappedByteBufferReader
     * 
     * @param buffer 映射的字节缓冲区
     */
    public MappedByteBufferReader(MappedByteBuffer buffer) {
        this.buffer = buffer; // 设置映射的字节缓冲区
        this.limit = 0; // 初始化限制
        this.position = 0; // 初始化读取位置
        this.capacity = buffer.capacity(); // 获取缓冲区容量
        this.leftLength = this.capacity; // 设置剩余长度
        this.buf = new byte[MAX_BUFFER_LENGTH]; // 创建字节数组
    }

    @Override
    public int readBinary() throws IOException {
        if (!remaining() && !reFill()) {
            throw new EOFException(); // 抛出EOF异常
        }
        return buf[position++] & 0xFF; // 返回读取的字节
    }

    @Override
    public int readBinary(byte[] bytes) throws IOException {
        return readBinary(bytes, bytes.length); // 读取字节数组
    }

    @Override
    public int readBinary(byte[] bytes, int length) throws IOException {
        for (int i = 0; i < length; ) {
            if (!remaining() && !reFill()) {
                throw new EOFException(); // 抛出EOF异常
            }
            int pending = length - i; // 剩余字节数
            int fillLength = Math.min(pending, this.limit - position); // 可填充的字节数
            if (fillLength > 0) {
                System.arraycopy(buf, position, bytes, i, fillLength); // 从缓冲区复制字节
                i += fillLength; // 更新已读取字节数
                this.position += fillLength; // 更新当前读取位置
            }
        }
        return 0; // 返回0表示没有数据
    }

    @Override
    public void readBinary(BuffedReadWriter buffedReadWriter, int length) throws IOException {
        for (int i = 0; i < length; ) {
            if (!remaining() && !reFill()) {
                throw new EOFException(); // 抛出EOF异常
            }
            int pending = length - i; // 剩余字节数
            int fillLength = Math.min(pending, this.limit - position); // 可填充的字节数
            if (fillLength > 0) {
                buffedReadWriter.writeBinary(buf, position, fillLength); // 将字节写入目标
                i += fillLength; // 更新已读取字节数
                this.position += fillLength; // 更新当前读取位置
            }
        }
    }

    @Override
    public void rewind() {
        // 该方法未实现
    }

    @Override
    public void clear() {
        // 该方法未实现
    }

    private boolean remaining() {
        return this.position < this.limit; // 检查是否还有剩余字节
    }

    private boolean reFill() {
        int readLength = Math.min(this.leftLength, this.buf.length); // 获取可读取的长度
        if (readLength <= 0) return false; // 如果没有可读取的长度，返回false
        this.buffer.get(this.buf, 0, readLength); // 从映射缓冲区读取字节
        this.leftLength -= readLength; // 更新剩余长度
        this.position = 0; // 重置读取位置
        this.limit = readLength; // 更新限制
        return true; // 返回true表示成功
    }
}
