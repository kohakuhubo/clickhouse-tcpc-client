package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.misc.BytesHelper;
import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

import java.io.IOException;

import static com.berry.clickhouse.tcp.client.settings.ClickHouseDefines.CHECKSUM_LENGTH;
import static com.berry.clickhouse.tcp.client.settings.ClickHouseDefines.COMPRESSION_HEADER_LENGTH;

public class CompressedBuffedReader implements BuffedReader, BytesHelper {

    private int position;
    private int capacity;
    private byte[] decompressed;

    private final BuffedReader buf;

    private final Decompressor lz4Decompressor = new Lz4Decompressor();
    private final Decompressor zstdDecompressor = new ZstdDecompressor();

    public CompressedBuffedReader(BuffedReader buf) {
        this.buf = buf;
    }

    @Override
    public int readBinary() throws IOException {
        if (position == capacity) {
            decompressed = readCompressedData();
            this.position = 0;
            this.capacity = decompressed.length;
        }

        return decompressed[position++];
    }

    @Override
    public int readBinary(byte[] bytes) throws IOException {
        for (int i = 0; i < bytes.length; ) {
            if (position == capacity) {
                decompressed = readCompressedData();
                this.position = 0;
                this.capacity = decompressed.length;
            }

            int padding = bytes.length - i;
            int fillLength = Math.min(padding, capacity - position);

            if (fillLength > 0) {
                System.arraycopy(decompressed, position, bytes, i, fillLength);

                i += fillLength;
                this.position += fillLength;
            }
        }
        return bytes.length;
    }

    @Override
    public void rewind() {

    }

    @Override
    public void clear() {

    }

    // @formatter:off
    private static final int NONE = 0x02;
    private static final int LZ4  = 0x82;
    private static final int ZSTD = 0x90;
    // @formatter:on

    private byte[] readCompressedData() throws IOException {
        buf.readBinary(new byte[CHECKSUM_LENGTH]);

        byte[] compressedHeader = new byte[COMPRESSION_HEADER_LENGTH];

        if (buf.readBinary(compressedHeader) != COMPRESSION_HEADER_LENGTH) {
            throw new IOException("Invalid compression header");
        }

        int method = compressedHeader[0] & 0x0FF;
        int compressedSize = getIntLE(compressedHeader, 1);
        int decompressedSize = getIntLE(compressedHeader, 5);

        switch (method) {
            case LZ4:
                return readLZ4CompressedData(compressedSize - COMPRESSION_HEADER_LENGTH, decompressedSize);
            case NONE:
                return readNoneCompressedData(decompressedSize);
            default:
                throw new UnsupportedOperationException("Unknown compression magic code: " + method);
        }
    }

    private byte[] readNoneCompressedData(int size) throws IOException {
        byte[] decompressed = new byte[size];

        if (buf.readBinary(decompressed) != size) {
            throw new IOException("Cannot decompress use None method.");
        }

        return decompressed;
    }

    private byte[] readLZ4CompressedData(int compressedSize, int decompressedSize) throws IOException {
        byte[] compressed = new byte[compressedSize];
        if (buf.readBinary(compressed) == compressedSize) {
            byte[] decompressed = new byte[decompressedSize];

            if (lz4Decompressor.decompress(compressed, 0, compressedSize, decompressed, 0, decompressedSize) == decompressedSize) {
                return decompressed;
            }
        }

        throw new IOException("Cannot decompress use LZ4 method.");
    }
}
