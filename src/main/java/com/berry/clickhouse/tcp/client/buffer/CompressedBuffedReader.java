package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.misc.BytesHelper;
import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

import java.io.IOException;

import static com.berry.clickhouse.tcp.client.settings.ClickHouseDefines.CHECKSUM_LENGTH;
import static com.berry.clickhouse.tcp.client.settings.ClickHouseDefines.COMPRESSION_HEADER_LENGTH;

/**
 * CompressedBuffedReader类实现了BuffedReader接口
 * 用于从压缩的字节流中读取数据
 */
public class CompressedBuffedReader implements BuffedReader, BytesHelper {

    private int position; // 当前读取位置
    private int capacity; // 当前容量
    private byte[] decompressed; // 解压后的字节数组

    private final BuffedReader buf; // 原始的BuffedReader

    private final Decompressor lz4Decompressor = new Lz4Decompressor(); // LZ4解压缩器
    private final Decompressor zstdDecompressor = new ZstdDecompressor(); // ZSTD解压缩器

    /**
     * 构造函数，初始化CompressedBuffedReader
     * 
     * @param buf 原始的BuffedReader
     */
    public CompressedBuffedReader(BuffedReader buf) {
        this.buf = buf; // 设置原始的BuffedReader
    }

    @Override
    public int readBinary() throws IOException {
        if (position == capacity) {
            decompressed = readCompressedData(); // 读取压缩数据
            this.position = 0; // 重置读取位置
            this.capacity = decompressed.length; // 更新容量
        }
        return decompressed[position++]; // 返回读取的字节
    }

    @Override
    public int readBinary(byte[] bytes) throws IOException {
        for (int i = 0; i < bytes.length; ) {
            if (position == capacity) {
                decompressed = readCompressedData(); // 读取压缩数据
                this.position = 0; // 重置读取位置
                this.capacity = decompressed.length; // 更新容量
            }
            int padding = bytes.length - i; // 剩余字节数
            int fillLength = Math.min(padding, capacity - position); // 可填充的字节数
            if (fillLength > 0) {
                System.arraycopy(decompressed, position, bytes, i, fillLength); // 从解压后的数据中复制字节
                i += fillLength; // 更新已读取字节数
                this.position += fillLength; // 更新当前读取位置
            }
        }
        return bytes.length; // 返回读取的字节数
    }

    @Override
    public void rewind() {
        // 该方法未实现
    }

    @Override
    public void clear() {
        // 该方法未实现
    }

    // 压缩方法常量
    private static final int NONE = 0x02;
    private static final int LZ4  = 0x82;
    private static final int ZSTD = 0x90;

    /**
     * 读取压缩数据
     * 
     * @return 解压后的字节数组
     * @throws IOException IO异常
     */
    private byte[] readCompressedData() throws IOException {
        buf.readBinary(new byte[CHECKSUM_LENGTH]); // 读取校验和

        byte[] compressedHeader = new byte[COMPRESSION_HEADER_LENGTH]; // 压缩头部

        if (buf.readBinary(compressedHeader) != COMPRESSION_HEADER_LENGTH) {
            throw new IOException("Invalid compression header"); // 抛出无效压缩头部异常
        }

        int method = compressedHeader[0] & 0x0FF; // 获取压缩方法
        int compressedSize = getIntLE(compressedHeader, 1); // 获取压缩数据大小
        int decompressedSize = getIntLE(compressedHeader, 5); // 获取解压后数据大小

        switch (method) {
            case LZ4:
                return readLZ4CompressedData(compressedSize - COMPRESSION_HEADER_LENGTH, decompressedSize); // 读取LZ4压缩数据
            case NONE:
                return readNoneCompressedData(decompressedSize); // 读取未压缩数据
            default:
                throw new UnsupportedOperationException("Unknown compression magic code: " + method); // 抛出未知压缩方法异常
        }
    }

    /**
     * 读取未压缩数据
     * 
     * @param size 数据大小
     * @return 解压后的字节数组
     * @throws IOException IO异常
     */
    private byte[] readNoneCompressedData(int size) throws IOException {
        byte[] decompressed = new byte[size]; // 创建解压后的字节数组

        if (buf.readBinary(decompressed) != size) {
            throw new IOException("Cannot decompress use None method."); // 抛出无法解压异常
        }

        return decompressed; // 返回解压后的字节数组
    }

    /**
     * 读取LZ4压缩数据
     * 
     * @param compressedSize 压缩数据大小
     * @param decompressedSize 解压后数据大小
     * @return 解压后的字节数组
     * @throws IOException IO异常
     */
    private byte[] readLZ4CompressedData(int compressedSize, int decompressedSize) throws IOException {
        byte[] compressed = new byte[compressedSize]; // 创建压缩数据字节数组
        if (buf.readBinary(compressed) == compressedSize) {
            byte[] decompressed = new byte[decompressedSize]; // 创建解压后的字节数组

            if (lz4Decompressor.decompress(compressed, 0, compressedSize, decompressed, 0, decompressedSize) == decompressedSize) {
                return decompressed; // 返回解压后的字节数组
            }
        }

        throw new IOException("Cannot decompress use LZ4 method."); // 抛出无法解压异常
    }
}
