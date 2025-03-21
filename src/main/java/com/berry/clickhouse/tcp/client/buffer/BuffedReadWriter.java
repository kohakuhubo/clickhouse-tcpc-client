package com.berry.clickhouse.tcp.client.buffer;

import java.io.IOException;

public interface BuffedReadWriter {

    void writeBinary(byte byt) throws IOException;

    void writeBinary(byte[] bytes) throws IOException;

    void writeBinary(byte[] bytes, int offset, int length) throws IOException;

    void writeBinaryReverse(byte[] bytes) throws IOException;

    void writeBinaryReverse(byte[] bytes, int offset, int length) throws IOException;

    void flushToTarget(boolean force) throws IOException;

    int readBinary() throws IOException;

    int readBinary(byte[] bytes) throws IOException;

    void rewind();

    void clear();

}
