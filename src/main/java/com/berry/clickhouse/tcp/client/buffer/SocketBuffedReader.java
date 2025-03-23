package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.settings.ClickHouseDefines;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

/**
 * SocketBuffedReader类实现了BuffedReader接口
 * 用于从Socket输入流中读取数据
 */
public class SocketBuffedReader implements BuffedReader {

    private final int capacity; // 缓冲区容量
    private final byte[] buf; // 缓冲区字节数组
    private final InputStream in; // 输入流

    private int limit; // 当前限制
    private int position; // 当前读取位置

    /**
     * 构造函数，初始化SocketBuffedReader
     * 
     * @param socket Socket实例
     * @throws IOException IO异常
     */
    public SocketBuffedReader(Socket socket) throws IOException {
        this(socket.getInputStream(), ClickHouseDefines.SOCKET_RECV_BUFFER_BYTES); // 获取Socket输入流
    }

    SocketBuffedReader(InputStream in, int capacity) {
        this.limit = 0; // 初始化限制
        this.position = 0; // 初始化读取位置
        this.capacity = capacity; // 设置缓冲区容量
        this.in = in; // 设置输入流
        this.buf = new byte[capacity]; // 创建字节数组
    }

    @Override
    public int readBinary() throws IOException {
        if (!remaining() && !refill()) {
            throw new EOFException("Attempt to read after eof."); // 抛出EOF异常
        }
        return buf[position++] & 0xFF; // 返回读取的字节
    }

    @Override
    public int readBinary(byte[] bytes) throws IOException {
        for (int i = 0; i < bytes.length; ) {
            if (!remaining() && !refill()) {
                throw new EOFException("Attempt to read after eof."); // 抛出EOF异常
            }
            int pending = bytes.length - i; // 剩余字节数
            int fillLength = Math.min(pending, limit - position); // 可填充的字节数
            if (fillLength > 0) {
                System.arraycopy(buf, position, bytes, i, fillLength); // 从缓冲区复制字节
                i += fillLength; // 更新已读取字节数
                this.position += fillLength; // 更新当前读取位置
            }
        }
        return bytes.length; // 返回读取的字节数
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
        return position < limit; // 检查是否还有剩余字节
    }

    private boolean refill() throws IOException {
        if (!remaining() && (limit = in.read(buf, 0, capacity)) <= 0) {
            throw new EOFException("Attempt to read after eof."); // 抛出EOF异常
        }
        position = 0; // 重置读取位置
        return true; // 返回true表示成功
    }
}
