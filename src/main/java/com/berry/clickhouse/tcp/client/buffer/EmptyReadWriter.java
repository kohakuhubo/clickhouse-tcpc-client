package com.berry.clickhouse.tcp.client.buffer;

import java.io.IOException;

public class EmptyReadWriter implements BuffedReadWriter {

    public static final EmptyReadWriter DEFAULT = new EmptyReadWriter();

    @Override
    public void writeBinary(byte byt) throws IOException {

    }

    @Override
    public void writeBinary(byte[] bytes) throws IOException {

    }

    @Override
    public void writeBinary(byte[] bytes, int offset, int length) throws IOException {

    }

    @Override
    public void writeBinaryReverse(byte[] bytes) throws IOException {

    }

    @Override
    public void writeBinaryReverse(byte[] bytes, int offset, int length) throws IOException {

    }

    @Override
    public void flushToTarget(boolean force) throws IOException {

    }

    @Override
    public int readBinary() throws IOException {
        return 0;
    }

    @Override
    public int readBinary(byte[] bytes) throws IOException {
        return 0;
    }

    @Override
    public void rewind() {

    }

    @Override
    public void clear() {

    }
}
