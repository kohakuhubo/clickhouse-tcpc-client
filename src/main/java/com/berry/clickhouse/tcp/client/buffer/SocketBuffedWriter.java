package com.berry.clickhouse.tcp.client.buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * SocketBuffedWriter类实现了BuffedWriter接口
 * 用于将数据写入Socket输出流
 */
public class SocketBuffedWriter implements BuffedWriter {

    private final OutputStream out; // 输出流

    /**
     * 构造函数，初始化SocketBuffedWriter
     * 
     * @param socket Socket实例
     * @throws IOException IO异常
     */
    public SocketBuffedWriter(Socket socket) throws IOException {
        this.out = socket.getOutputStream(); // 获取Socket输出流
    }

    @Override
    public void writeBinary(byte byt) throws IOException {
        out.write(byt); // 写入字节
    }

    @Override
    public void writeBinary(byte[] bytes, int offset, int length) throws IOException {
        out.write(bytes, offset, length); // 写入字节数组
    }

    @Override
    public void flushToTarget(boolean force) throws IOException {
        out.flush(); // 刷新输出流
    }
}
