package com.berry.clickhouse.tcp.client.buffer;

import java.io.IOException;
import java.io.InputStream;

/**
 * BuffedReader类用于从输入流中读取数据
 * 提供缓冲读取功能以提高读取效率
 */
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
