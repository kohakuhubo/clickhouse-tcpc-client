package com.berry.clickhouse.tcp.client.buffer;

import com.berry.clickhouse.tcp.client.misc.BytesHelper;
import com.berry.clickhouse.tcp.client.misc.ClickHouseCityHash;
import io.airlift.compress.Compressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.zstd.ZstdCompressor;

import java.io.IOException;

import static com.berry.clickhouse.tcp.client.settings.ClickHouseDefines.CHECKSUM_LENGTH;
import static com.berry.clickhouse.tcp.client.settings.ClickHouseDefines.COMPRESSION_HEADER_LENGTH;

/**
 * CompressedBuffedWriter类实现了BuffedWriter接口
 * 用于将数据写入压缩的字节流
 */
public class CompressedBuffedWriter implements BuffedWriter, BytesHelper {

    private final int capacity; // 缓冲区容量
    private final byte[] writtenBuf; // 写入的字节缓冲区
    private final BuffedWriter writer; // 原始的BuffedWriter

    private final Compressor lz4Compressor = new Lz4Compressor(); // LZ4压缩器
    private final Compressor zstdCompressor = new ZstdCompressor(); // ZSTD压缩器

    private int position; // 当前写入位置

    /**
     * 构造函数，初始化CompressedBuffedWriter
     * 
     * @param capacity 缓冲区容量
     * @param writer 原始的BuffedWriter
     */
    public CompressedBuffedWriter(int capacity, BuffedWriter writer) {
        this.capacity = capacity; // 设置缓冲区容量
        this.writtenBuf = new byte[capacity]; // 创建写入的字节缓冲区
        this.writer = writer; // 设置原始的BuffedWriter
    }

    @Override
    public void writeBinary(byte byt) throws IOException {
        writtenBuf[position++] = byt; // 写入字节
        flushToTarget(false); // 刷新到目标
    }

    @Override
    public void writeBinary(byte[] bytes) throws IOException {
        // 该方法未实现
    }

    @Override
    public void writeBinary(byte[] bytes, int offset, int length) throws IOException {
        while (remaining() < length) {
            int num = remaining(); // 获取剩余空间
            System.arraycopy(bytes, offset, writtenBuf, position, remaining()); // 写入剩余字节
            position += num; // 更新写入位置
            flushToTarget(false); // 刷新到目标
            offset += num; // 更新偏移量
            length -= num; // 更新长度
        }
        System.arraycopy(bytes, offset, writtenBuf, position, length); // 写入剩余字节
        position += length; // 更新写入位置
        flushToTarget(false); // 刷新到目标
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
            int maxLen = lz4Compressor.maxCompressedLength(position); // 获取最大压缩长度

            byte[] compressedBuffer = new byte[maxLen + COMPRESSION_HEADER_LENGTH + CHECKSUM_LENGTH]; // 创建压缩缓冲区
            int res = lz4Compressor.compress(writtenBuf, 0, position, compressedBuffer, COMPRESSION_HEADER_LENGTH + CHECKSUM_LENGTH, compressedBuffer.length); // 压缩数据

            compressedBuffer[CHECKSUM_LENGTH] = (byte) (0x82 & 0xFF); // 设置压缩方法
            int compressedSize = res + COMPRESSION_HEADER_LENGTH; // 更新压缩数据大小
            System.arraycopy(getBytesLE(compressedSize), 0, compressedBuffer, CHECKSUM_LENGTH + 1, Integer.BYTES); // 写入压缩大小
            System.arraycopy(getBytesLE(position), 0, compressedBuffer, CHECKSUM_LENGTH + Integer.BYTES + 1, Integer.BYTES); // 写入原始数据大小

            long[] checksum = ClickHouseCityHash.cityHash128(compressedBuffer, CHECKSUM_LENGTH, compressedSize); // 计算校验和
            System.arraycopy(getBytesLE(checksum[0]), 0, compressedBuffer, 0, Long.BYTES); // 写入校验和
            System.arraycopy(getBytesLE(checksum[1]), 0, compressedBuffer, Long.BYTES, Long.BYTES); // 写入校验和

            writer.writeBinary(compressedBuffer, 0, compressedSize + CHECKSUM_LENGTH); // 写入压缩数据
            position = 0; // 重置写入位置
        }
    }

    private boolean hasRemaining() {
        return position < capacity; // 检查是否还有剩余空间
    }

    private int remaining() {
        return capacity - position; // 返回剩余空间
    }
}
