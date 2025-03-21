package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.ByteArrayWriter;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;

public class ColumnWriterBuffer {

    private final ByteArrayWriter columnWriter;

    public final BinarySerializer column;

    private final boolean isUseSysColumnWriterBuffer;

    public ColumnWriterBuffer(int size, int length, Queue<ByteBuffer> systemByteBufferQueue) {
        this.isUseSysColumnWriterBuffer = (null != systemByteBufferQueue);
        this.columnWriter = new ByteArrayWriter(size, length, systemByteBufferQueue);
        this.column = new BinarySerializer(columnWriter, false);
    }

    public void writeTo(BinarySerializer serializer) throws IOException {
        for (ByteBuffer buffer : columnWriter.getBufferList()) {
            serializer.writeBytes(buffer.array(), buffer.arrayOffset(), buffer.position());
            buffer.limit(buffer.position());
        }
    }

    public void reset() {
        columnWriter.reset();
    }

    public void clear() {
        columnWriter.clear();
    }

    public void rewind() {
        columnWriter.rewind();
    }

    public boolean isUseSysColumnWriterBuffer() {
        return isUseSysColumnWriterBuffer;
    }
}
