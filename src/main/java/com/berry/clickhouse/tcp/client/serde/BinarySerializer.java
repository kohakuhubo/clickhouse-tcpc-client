/**
 * ClickHouse二进制序列化器
 * 用于将数据编码为ClickHouse二进制格式并写入流
 * 支持数据压缩功能
 */
package com.berry.clickhouse.tcp.client.serde;

import com.berry.clickhouse.tcp.client.buffer.BuffedWriter;
import com.berry.clickhouse.tcp.client.buffer.CompressedBuffedWriter;
import com.berry.clickhouse.tcp.client.misc.Switcher;
import com.berry.clickhouse.tcp.client.settings.ClickHouseDefines;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * 二进制序列化器类
 * 实现了ClickHouse二进制协议的数据编码
 * 提供各种类型数据的序列化方法
 */
public class BinarySerializer {

    /**
     * 输出流切换器，用于在普通流和压缩流之间切换
     */
    private final Switcher<BuffedWriter> switcher;
    
    /**
     * 是否启用压缩
     */
    private final boolean enableCompress;
    
    /**
     * 写入缓冲区
     */
    private final byte[] writeBuffer;
    
    /**
     * 创建一个新的二进制序列化器
     * 
     * @param writer 缓冲写入器
     * @param enableCompress 是否启用压缩
     */
    public BinarySerializer(BuffedWriter writer, boolean enableCompress) {
        this.enableCompress = enableCompress;
        BuffedWriter compressWriter = null;
        if (enableCompress) {
            compressWriter = new CompressedBuffedWriter(ClickHouseDefines.SOCKET_SEND_BUFFER_BYTES, writer);
        }
        switcher = new Switcher<>(compressWriter, writer);
        writeBuffer = new byte[8];
    }

    /**
     * 写入变长整数
     * ClickHouse使用变长编码存储整数以节省空间
     * 
     * @param x 要写入的长整数
     * @throws IOException 如果写入失败
     */
    public void writeVarInt(long x) throws IOException {
        for (int i = 0; i < 9; i++) {
            byte byt = (byte) (x & 0x7F);

            if (x > 0x7F) {
                byt |= 0x80;
            }

            x >>= 7;
            switcher.get().writeBinary(byt);

            if (x == 0) {
                return;
            }
        }
    }

    /**
     * 写入单个字节
     * 
     * @param x 要写入的字节
     * @throws IOException 如果写入失败
     */
    public void writeByte(byte x) throws IOException {
        switcher.get().writeBinary(x);
    }

    /**
     * 写入布尔值
     * 
     * @param x 要写入的布尔值
     * @throws IOException 如果写入失败
     */
    public void writeBoolean(boolean x) throws IOException {
        writeVarInt((byte) (x ? 1 : 0));
    }

    /**
     * 写入短整数
     * 
     * @param i 要写入的短整数
     * @throws IOException 如果写入失败
     */
    @SuppressWarnings("PointlessBitwiseExpression")
    public void writeShort(short i) throws IOException {
        // @formatter:off
        writeBuffer[0] = (byte) ((i >> 0) & 0xFF);
        writeBuffer[1] = (byte) ((i >> 8) & 0xFF);
        switcher.get().writeBinary(writeBuffer, 0, 2);
        // @formatter:on
    }

    /**
     * 写入整数
     * 
     * @param i 要写入的整数
     * @throws IOException 如果写入失败
     */
    @SuppressWarnings("PointlessBitwiseExpression")
    public void writeInt(int i) throws IOException {
        // @formatter:off
        writeBuffer[0] = (byte) ((i >> 0)  & 0xFF);
        writeBuffer[1] = (byte) ((i >> 8)  & 0xFF);
        writeBuffer[2] = (byte) ((i >> 16) & 0xFF);
        writeBuffer[3] = (byte) ((i >> 24) & 0xFF);
        switcher.get().writeBinary(writeBuffer, 0, 4);
        // @formatter:on
    }

    /**
     * 写入长整数
     * 
     * @param i 要写入的长整数
     * @throws IOException 如果写入失败
     */
    @SuppressWarnings("PointlessBitwiseExpression")
    public void writeLong(long i) throws IOException {
        // @formatter:off
        writeBuffer[0] = (byte) ((i >> 0)  & 0xFF);
        writeBuffer[1] = (byte) ((i >> 8)  & 0xFF);
        writeBuffer[2] = (byte) ((i >> 16) & 0xFF);
        writeBuffer[3] = (byte) ((i >> 24) & 0xFF);
        writeBuffer[4] = (byte) ((i >> 32) & 0xFF);
        writeBuffer[5] = (byte) ((i >> 40) & 0xFF);
        writeBuffer[6] = (byte) ((i >> 48) & 0xFF);
        writeBuffer[7] = (byte) ((i >> 56) & 0xFF);
        switcher.get().writeBinary(writeBuffer, 0, 8);
        // @formatter:on
    }

    /**
     * 写入UTF-8编码的字符串
     * 
     * @param utf8 要写入的字符串
     * @throws IOException 如果写入失败
     */
    public void writeUTF8StringBinary(String utf8) throws IOException {
        writeStringBinary(utf8, StandardCharsets.UTF_8);
    }

    /**
     * 写入指定字符集编码的字符串
     * 
     * @param data 要写入的字符串
     * @param charset 字符集
     * @throws IOException 如果写入失败
     */
    public void writeStringBinary(String data, Charset charset) throws IOException {
        byte[] bs = data.getBytes(charset);
        writeBytesBinary(bs);
    }

    /**
     * 写入字节数组
     * 
     * @param bs 要写入的字节数组
     * @throws IOException 如果写入失败
     */
    public void writeBytesBinary(byte[] bs) throws IOException {
        writeVarInt(bs.length);
        switcher.get().writeBinary(bs, 0, bs.length);
    }

    /**
     * 将数据刷新到目标流
     * 
     * @param force 是否强制刷新
     * @throws IOException 如果刷新失败
     */
    public void flushToTarget(boolean force) throws IOException {
        switcher.get().flushToTarget(force);
    }

    /**
     * 启用压缩（如果已配置）
     */
    public void maybeEnableCompressed() {
        if (enableCompress) {
            switcher.select(true);
        }
    }

    /**
     * 禁用压缩并刷新数据
     * 
     * @throws IOException 如果操作失败
     */
    public void maybeDisableCompressed() throws IOException {
        if (enableCompress) {
            switcher.get().flushToTarget(true);
            switcher.select(false);
        }
    }

    /**
     * 写入浮点数
     * 
     * @param datum 要写入的浮点数
     * @throws IOException 如果写入失败
     */
    public void writeFloat(float datum) throws IOException {
        int x = Float.floatToIntBits(datum);
        writeInt(x);
    }

    /**
     * 写入双精度浮点数
     * 
     * @param datum 要写入的双精度浮点数
     * @throws IOException 如果写入失败
     */
    @SuppressWarnings("PointlessBitwiseExpression")
    public void writeDouble(double datum) throws IOException {
        long x = Double.doubleToLongBits(datum);
        // @formatter:off
        writeBuffer[0] = (byte) ((x >>> 0)  & 0xFF);
        writeBuffer[1] = (byte) ((x >>> 8)  & 0xFF);
        writeBuffer[2] = (byte) ((x >>> 16) & 0xFF);
        writeBuffer[3] = (byte) ((x >>> 24) & 0xFF);
        writeBuffer[4] = (byte) ((x >>> 32) & 0xFF);
        writeBuffer[5] = (byte) ((x >>> 40) & 0xFF);
        writeBuffer[6] = (byte) ((x >>> 48) & 0xFF);
        writeBuffer[7] = (byte) ((x >>> 56) & 0xFF);
        switcher.get().writeBinary(writeBuffer, 0, 8);
        // @formatter:on
    }

    /**
     * 写入字节数组（不包含长度前缀）
     * 
     * @param bytes 要写入的字节数组
     * @throws IOException 如果写入失败
     */
    public void writeBytes(byte[] bytes) throws IOException {
        writeBytes(bytes, 0, bytes.length);
    }
    
    /**
     * 写入字节数组的指定部分
     * 
     * @param bytes 要写入的字节数组
     * @param offset 起始偏移量
     * @param length 写入长度
     * @throws IOException 如果写入失败
     */
    public void writeBytes(byte[] bytes, int offset, int length) throws IOException {
        switcher.get().writeBinary(bytes, offset, length);
    }
}
