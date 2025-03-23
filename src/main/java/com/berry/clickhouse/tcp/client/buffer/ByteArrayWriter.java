package com.berry.clickhouse.tcp.client.buffer;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * ByteArrayWriter类实现了BuffedWriter和BuffedReader接口
 * 用于在字节数组中写入和读取数据
 */
public class ByteArrayWriter implements BuffedWriter, BuffedReader {

    private int nextReadBuffer; // 下一个读取的缓冲区索引
    private ByteBuffer buffer; // 当前缓冲区
    private final List<ByteBuffer> byteBufferList = new LinkedList<>(); // 存储字节缓冲区列表
    private final Deque<ByteBuffer> freeList = new LinkedList<>(); // 存储空闲缓冲区的队列
    private final int freeListSize; // 空闲缓冲区大小
    private final Supplier<ByteBuffer> allocateBuffer; // 分配缓冲区的供应商
    private final Consumer<ByteBuffer> recycleBuffer; // 回收缓冲区的消费者

    /**
     * 构造函数，初始化ByteArrayWriter
     * 
     * @param freeListSize 空闲缓冲区大小
     * @param allocateBuffer 分配缓冲区的供应商
     * @param recycleBuffer 回收缓冲区的消费者
     */
    public ByteArrayWriter(int freeListSize, Supplier<ByteBuffer> allocateBuffer, Consumer<ByteBuffer> recycleBuffer) {
        reuseOrAllocateByteBuffer(); // 复用或分配字节缓冲区
        this.freeListSize = freeListSize; // 设置空闲缓冲区大小
        this.allocateBuffer = allocateBuffer; // 设置分配缓冲区的供应商
        this.recycleBuffer = recycleBuffer; // 设置回收缓冲区的消费者
    }

    public ByteArrayWriter(Supplier<ByteBuffer> allocateBuffer, Consumer<ByteBuffer> recycleBuffer) {
        this(-1, allocateBuffer, recycleBuffer);
    }

    @Override
    public void writeBinary(byte byt) throws IOException {
        flushToTarget(false); // 刷新到目标
        buffer.put(byt); // 写入字节
    }

    @Override
    public void writeBinary(byte[] bytes) throws IOException {
        writeBinary(bytes, 0, bytes.length); // 写入字节数组
    }

    @Override
    public void writeBinary(byte[] bytes, int offset, int length) throws IOException {
        if (length <= 0) return; // 如果长度小于等于0，直接返回
        flushToTarget(false); // 刷新到目标
        while (buffer.remaining() < length) {
            int num = buffer.remaining(); // 获取剩余空间
            buffer.put(bytes, offset, num); // 写入剩余字节
            flushToTarget(true); // 刷新到目标
            offset += num; // 更新偏移量
            length -= num; // 更新长度
        }
        buffer.put(bytes, offset, length); // 写入剩余字节
    }

    @Override
    public void writeBinaryReverse(byte[] bytes) throws IOException {
        writeBinaryReverse(bytes, 0, bytes.length);
    }

    @Override
    public void writeBinaryReverse(byte[] bytes, int offset, int length) throws IOException {
        if (length <= 0)
            return;

        //写入前检查内存
        flushToTarget(false);
        while (buffer.remaining() < length) {
            int num = buffer.remaining();
            writeReverse(bytes, offset, num);
            flushToTarget(true);

            length -= num;
        }

        writeReverse(bytes, offset, length);
    }

    @Override
    public void flushToTarget(boolean force) throws IOException {
        if (buffer.hasRemaining() && !force) {
            return; // 如果缓冲区还有剩余且不强制刷新，直接返回
        }
        reuseOrAllocateByteBuffer(); // 复用或分配字节缓冲区
    }

    public List<ByteBuffer> getBufferList() {
        return byteBufferList;
    }

    public void reset() {

        if (this.freeListSize < 0) {
            byteBufferList.forEach(b -> {
                // upcast is necessary, see detail at:
                // https://bitbucket.org/ijabz/jaudiotagger/issues/313/java-8-javalangnosuchmethoderror
                ((Buffer) b).clear();
                freeList.addLast(b);
            });
        } else {
            for (int i = 0; i < byteBufferList.size(); i++) {
                if (i >= this.freeListSize) {
                    ByteBuffer b = byteBufferList.get(i);
                    this.recycleBuffer.accept(b);
                } else {
                    ByteBuffer b = byteBufferList.get(i);
                    ((Buffer) b).clear();
                    freeList.addLast(b);
                }
            }
        }
        byteBufferList.clear();
        reuseOrAllocateByteBuffer();
    }

    private ByteBuffer reuseOrAllocateByteBuffer() {
        ByteBuffer newBuffer = freeList.pollLast(); // 从空闲列表中获取缓冲区
        if (newBuffer == null) {
            newBuffer = allocateBuffer.get(); // 分配新的缓冲区
        }
        buffer = newBuffer; // 设置当前缓冲区
        byteBufferList.add(buffer); // 添加到字节缓冲区列表
        return buffer; // 返回当前缓冲区
    }

    @Override
    public int readBinary() throws IOException {
        if (this.buffer.position() == this.buffer.limit()) {
            if (this.nextReadBuffer == this.byteBufferList.size()) {
                throw new IOException("read error! don`t has more byte buffer!"); // 抛出读取错误
            }
            this.buffer = this.byteBufferList.get(this.nextReadBuffer); // 获取下一个缓冲区
            this.buffer.rewind(); // 重置缓冲区位置
            this.nextReadBuffer++; // 更新下一个读取的缓冲区索引
        }
        return this.buffer.get(); // 返回读取的字节
    }

    @Override
    public int readBinary(byte[] bytes) throws IOException {
        for (int i = 0; i < bytes.length; ) {
            if (this.buffer.position() == this.buffer.limit()) {
                if (this.nextReadBuffer == this.byteBufferList.size()) {
                    throw new IOException("read error! don`t has more byte buffer!"); // 抛出读取错误
                }
                this.buffer = this.byteBufferList.get(this.nextReadBuffer); // 获取下一个缓冲区
                this.buffer.rewind(); // 重置缓冲区位置
                this.nextReadBuffer++; // 更新下一个读取的缓冲区索引
            }
            int padding = bytes.length - i; // 剩余字节数
            int fillLength = Math.min(padding, this.buffer.capacity() - this.buffer.position()); // 可填充的字节数
            if (fillLength > 0) {
                this.buffer.get(bytes, i, fillLength); // 从缓冲区读取字节
                i += fillLength; // 更新已读取字节数
            }
        }
        return bytes.length; // 返回读取的字节数
    }

    @Override
    public void rewind() {
        this.nextReadBuffer = 1; // 重置下一个读取的缓冲区索引
        this.buffer = this.byteBufferList.get(0); // 获取第一个缓冲区
        this.buffer.rewind(); // 重置缓冲区位置
    }

    @Override
    public void clear() {
        this.nextReadBuffer = 1; // 重置下一个读取的缓冲区索引
        this.byteBufferList.forEach(ByteBuffer::clear); // 清空所有缓冲区
        this.buffer = this.byteBufferList.get(0); // 获取第一个缓冲区
    }

    public void writeReverse(byte[] bytes, int offset, int length) {
        int start = (offset + length) - 1;
        for (int i = start; i >= offset; i--) {
            buffer.put(bytes[i]);
        }
    }
}
