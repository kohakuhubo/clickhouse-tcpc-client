package com.berry.clickhouse.tcp.client.buffer;


import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;

public class MappedByteBufferReader implements BuffedReader {

    private static final int MAX_BUFFER_LENGTH = 2 * 1024 * 1024;

    private static int capacity;

    private int leftLength;

    private final byte[] buf;

    private MappedByteBuffer buffer;

    private int limit;

    private int position;

    public MappedByteBufferReader(MappedByteBuffer buffer) {
        this.buffer = buffer;
        this.limit = 0;
        this.position = 0;
        this.buffer = buffer;
        this.capacity = buffer.capacity();
        this.leftLength = this.capacity;
        this.buf = new byte[MAX_BUFFER_LENGTH];
    }

    @Override
    public int readBinary() throws IOException {
        if (!remaining() && !reFill())
            throw new EOFException();
        return buf[position++] & 0xFF;
    }

    @Override
    public int readBinary(byte[] bytes) throws IOException {
        return readBinary(bytes, bytes.length);
    }

    @Override
    public int readBinary(byte[] bytes, int length) throws IOException {
        for (int i = 0; i < length; ) {
            if (!remaining() && !reFill())
                throw new EOFException();
            int pending = length - i;
            int fillLength = Math.min(pending, this.limit - position);
            if (fillLength > 0) {
                System.arraycopy(buf, position, bytes, i, fillLength);
                i += fillLength;
                this.position += fillLength;
            }
        }
        return 0;
    }

    @Override
    public void readBinary(BuffedReadWriter buffedReadWriter, int length) throws IOException {
        for (int i = 0; i < length; ) {
            if (!remaining() && !reFill())
                throw new EOFException();
            int pending = length - i;
            int fillLength = Math.min(pending, this.limit - position);
            if (fillLength > 0) {
                buffedReadWriter.writeBinary(buf, position, fillLength);
                i += fillLength;
                this.position += fillLength;
            }
        }
    }

    @Override
    public void rewind() {

    }

    @Override
    public void clear() {

    }

    private boolean remaining() {
        return this.position < this.limit;
    }

    private boolean reFill() {
        int readLength = Math.min(this.leftLength, this.buf.length);
        if (readLength <= 0)
            return false;
        this.buffer.get(this.buf, 0, readLength);
        this.leftLength -= readLength;
        this.position = 0;
        this.limit = readLength;
        return true;
    }
}
