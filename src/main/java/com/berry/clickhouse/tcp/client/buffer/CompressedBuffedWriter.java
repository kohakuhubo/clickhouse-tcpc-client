package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.misc.BytesHelper;
import com.berry.clickhouse.tcp.client.misc.ClickHouseCityHash;
import io.airlift.compress.Compressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.zstd.ZstdCompressor;

import java.io.IOException;

import static com.berry.clickhouse.tcp.client.settings.ClickHouseDefines.CHECKSUM_LENGTH;
import static com.berry.clickhouse.tcp.client.settings.ClickHouseDefines.COMPRESSION_HEADER_LENGTH;

public class CompressedBuffedWriter implements BuffedWriter, BytesHelper {

    private final int capacity;
    private final byte[] writtenBuf;
    private final BuffedWriter writer;

    private final Compressor lz4Compressor = new Lz4Compressor();
    private final Compressor zstdCompressor = new ZstdCompressor();

    private int position;

    public CompressedBuffedWriter(int capacity, BuffedWriter writer) {
        this.capacity = capacity;
        this.writtenBuf = new byte[capacity];
        this.writer = writer;
    }

    @Override
    public void writeBinary(byte byt) throws IOException {
        writtenBuf[position++] = byt;
        flushToTarget(false);
    }

    @Override
    public void writeBinary(byte[] bytes) throws IOException {

    }

    @Override
    public void writeBinary(byte[] bytes, int offset, int length) throws IOException {
        while (remaining() < length) {
            int num = remaining();
            System.arraycopy(bytes, offset, writtenBuf, position, remaining());
            position += num;

            flushToTarget(false);
            offset += num;
            length -= num;
        }

        System.arraycopy(bytes, offset, writtenBuf, position, length);
        position += length;
        flushToTarget(false);
    }

    @Override
    public void writeBinaryReverse(byte[] bytes) throws IOException {

    }

    @Override
    public void writeBinaryReverse(byte[] bytes, int offset, int length) throws IOException {

    }

    @Override
    public void flushToTarget(boolean force) throws IOException {
        if (position > 0 && (force || !hasRemaining())) {
            int maxLen = lz4Compressor.maxCompressedLength(position);

            byte[] compressedBuffer = new byte[maxLen + COMPRESSION_HEADER_LENGTH + CHECKSUM_LENGTH];
            int res = lz4Compressor.compress(writtenBuf, 0, position, compressedBuffer, COMPRESSION_HEADER_LENGTH + CHECKSUM_LENGTH, compressedBuffer.length);

            compressedBuffer[CHECKSUM_LENGTH] = (byte) (0x82 & 0xFF);
            int compressedSize = res + COMPRESSION_HEADER_LENGTH;
            System.arraycopy(getBytesLE(compressedSize), 0, compressedBuffer, CHECKSUM_LENGTH + 1, Integer.BYTES);
            System.arraycopy(getBytesLE(position), 0, compressedBuffer, CHECKSUM_LENGTH + Integer.BYTES + 1, Integer.BYTES);

            long[] checksum = ClickHouseCityHash.cityHash128(compressedBuffer, CHECKSUM_LENGTH, compressedSize);
            System.arraycopy(getBytesLE(checksum[0]), 0, compressedBuffer, 0, Long.BYTES);
            System.arraycopy(getBytesLE(checksum[1]), 0, compressedBuffer, Long.BYTES, Long.BYTES);

            writer.writeBinary(compressedBuffer, 0, compressedSize + CHECKSUM_LENGTH);
            position = 0;
        }
    }

    private boolean hasRemaining() {
        return position < capacity;
    }

    private int remaining() {
        return capacity - position;
    }
}
