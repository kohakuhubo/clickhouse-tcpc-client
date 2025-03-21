package com.berry.clickhouse.tcp.client.buffer;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ByteArrayWriter implements BuffedWriter, BuffedReader {

    private int nextReadBuffer;

    private final int blockSize;

    private ByteBuffer buffer;

    private final List<ByteBuffer> byteBufferList = new LinkedList<>();

    // LIFO queue
    private final Deque<ByteBuffer> freeList = new LinkedList<>();

    private final Queue<ByteBuffer> systemFreeList;

    private final int freeListSize;

    public ByteArrayWriter(int blockSize, int freeListSize, Queue<ByteBuffer> systemFreeList) {
        this.blockSize = blockSize;
        reuseOrAllocateByteBuffer();
        this.freeListSize = freeListSize;
        this.systemFreeList = systemFreeList;
    }

    public ByteArrayWriter(int blockSize) {
        this(blockSize, -1, null);
    }

    @Override
    public void writeBinary(byte byt) throws IOException {
        flushToTarget(false);
        buffer.put(byt);
    }

    @Override
    public void writeBinary(byte[] bytes) throws IOException {
        writeBinary(bytes, 0, bytes.length);
    }

    @Override
    public void writeBinary(byte[] bytes, int offset, int length) throws IOException {
        if (length <= 0)
            return;
        //写入前检查内存
        flushToTarget(false);
        while (buffer.remaining() < length) {
            int num = buffer.remaining();
            buffer.put(bytes, offset, num);
            flushToTarget(true);

            offset += num;
            length -= num;
        }

        buffer.put(bytes, offset, length);
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
            return;
        }
        reuseOrAllocateByteBuffer();
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
                    if (null != this.systemFreeList) {
                        ByteBuffer b = byteBufferList.get(i);
                        ((Buffer) b).clear();
                        systemFreeList.offer(b);
                    } else {
                        break;
                    }
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
        ByteBuffer newBuffer = freeList.pollLast();
        if (newBuffer == null) {
            if (null != systemFreeList) {
                newBuffer = systemFreeList.poll();
                if (null == newBuffer) {
                    newBuffer = ByteBuffer.allocate(blockSize);
                }
            } else {
                newBuffer = ByteBuffer.allocate(blockSize);
            }
        }

        buffer = newBuffer;
        byteBufferList.add(buffer);
        return buffer;
    }

    @Override
    public int readBinary() throws IOException {
        if (this.buffer.position() == this.buffer.limit()) {
            if (this.nextReadBuffer == this.byteBufferList.size()) {
                throw new IOException("read error! don`t has more byte buffer!");
            }
            this.buffer = this.byteBufferList.get(this.nextReadBuffer);
            this.buffer.rewind();
            this.nextReadBuffer++;
        }
        return this.buffer.get();
    }

    @Override
    public int readBinary(byte[] bytes) throws IOException {
        for (int i = 0; i < bytes.length; ) {
            if (this.buffer.position() == this.buffer.limit()) {
                if (this.nextReadBuffer == this.byteBufferList.size()) {
                    throw new IOException("read error! don`t has more byte buffer!");
                }
                this.buffer = this.byteBufferList.get(this.nextReadBuffer);
                this.buffer.rewind();
                this.nextReadBuffer++;
            }

            int padding = bytes.length - i;
            int fillLength = Math.min(padding, this.buffer.capacity() - this.buffer.position());
            if (fillLength > 0) {
                this.buffer.get(bytes, i, fillLength);
                i += fillLength;
            }
        }
        return bytes.length;
    }

    @Override
    public void rewind() {
        this.nextReadBuffer = 1;
        this.buffer = this.byteBufferList.get(0);
        this.buffer.rewind();
    }

    @Override
    public void clear() {
        this.nextReadBuffer = 1;
        this.byteBufferList.forEach(ByteBuffer::clear);
        this.buffer = this.byteBufferList.get(0);
    }

    public void writeReverse(byte[] bytes, int offset, int length) {
        int start = (offset + length) - 1;
        for (int i = start; i >= offset; i--) {
            buffer.put(bytes[i]);
        }
    }
}
