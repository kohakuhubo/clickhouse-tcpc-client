package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.buffer.BufferPoolManager;
import com.berry.clickhouse.tcp.client.buffer.ByteArrayWriter;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ColumnWriterBuffer {

    private final ByteArrayWriter columnWriter;

    public final BinarySerializer column;

    public final ColumnWriterBufferPoolManager manager;

    public ColumnWriterBuffer(ColumnWriterBufferPoolManager manager, int length, Supplier<ByteBuffer> allocateBuffer, Consumer<ByteBuffer> recycleBuffer) {
        this.columnWriter = new ByteArrayWriter(length, allocateBuffer, recycleBuffer);
        this.column = new BinarySerializer(columnWriter, false);
        this.manager = manager;
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

    public ColumnWriterBufferPoolManager getManager() {
        return manager;
    }
}
