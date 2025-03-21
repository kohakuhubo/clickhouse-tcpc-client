package com.berry.clickhouse.tcp.client.buffer;

import java.io.IOException;

public interface BuffedReader {

    int readBinary() throws IOException;

    int readBinary(byte[] bytes) throws IOException;

    default int readBinary(byte[] bytes, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    default void readBinary(BuffedReadWriter buffedReadWriter, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    void rewind();

    void clear();
}
