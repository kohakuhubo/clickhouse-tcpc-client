/**
 * ClickHouse二进制反序列化器
 * 用于从二进制流中解码ClickHouse格式的数据
 * 支持数据解压缩功能
 */
package com.berry.clickhouse.tcp.client.serde;

import com.berry.clickhouse.tcp.client.buffer.BuffedReader;
import com.berry.clickhouse.tcp.client.buffer.CompressedBuffedReader;
import com.berry.clickhouse.tcp.client.misc.Switcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 二进制反序列化器类
 * 实现了ClickHouse二进制协议的数据解码
 * 提供各种类型数据的反序列化方法
 */
public class BinaryDeserializer {

    /**
     * 输入流切换器，用于在普通流和压缩流之间切换
     */
    private final Switcher<BuffedReader> switcher;
    
    /**
     * 是否启用压缩
     */
    private final boolean enableCompress;

    /**
     * 创建一个新的二进制反序列化器
     * 
     * @param buffedReader 缓冲读取器
     * @param enableCompress 是否启用压缩
     */
    public BinaryDeserializer(BuffedReader buffedReader, boolean enableCompress) {
        this.enableCompress = enableCompress;
        BuffedReader compressedReader = null;
        if (enableCompress) {
            compressedReader = new CompressedBuffedReader(buffedReader);
        }
        switcher = new Switcher<>(compressedReader, buffedReader);
    }

    /**
     * 读取变长整数
     * ClickHouse使用变长编码存储整数以节省空间
     * 
     * @return 读取的长整数
     * @throws IOException 如果读取失败或格式错误
     */
    public long readVarInt() throws IOException {
        long result = 0;
        for (int i = 0; i < 10; i++) {
            int currentByte = switcher.get().readBinary();
            long valueChunk = currentByte & 0x7F;
            result |= (valueChunk << (7 * i));
            if ((currentByte & 0x80) == 0) {
                return result;
            }
        }
        throw new IOException("Malformed VarInt: too long");
    }

    /**
     * 读取短整数
     * 
     * @return 读取的短整数
     * @throws IOException 如果读取失败
     */
    @SuppressWarnings("PointlessBitwiseExpression")
    public short readShort() throws IOException {
        return (short) (((switcher.get().readBinary() & 0xFF) << 0)
                      + ((switcher.get().readBinary() & 0xFF) << 8));
    }

    /**
     * 读取整数
     * 
     * @return 读取的整数
     * @throws IOException 如果读取失败
     */
    @SuppressWarnings("PointlessBitwiseExpression")
    public int readInt() throws IOException {
        // @formatter:off
        return ((switcher.get().readBinary() & 0xFF) << 0)
             + ((switcher.get().readBinary() & 0xFF) << 8)
             + ((switcher.get().readBinary() & 0xFF) << 16)
             + ((switcher.get().readBinary() & 0xFF) << 24);
        // @formatter:on
    }

    /**
     * 读取长整数
     * 
     * @return 读取的长整数
     * @throws IOException 如果读取失败
     */
    @SuppressWarnings("PointlessBitwiseExpression")
    public long readLong() throws IOException {
        // @formatter:off
        return ((switcher.get().readBinary() & 0xFFL) << 0)
             + ((switcher.get().readBinary() & 0xFFL) << 8)
             + ((switcher.get().readBinary() & 0xFFL) << 16)
             + ((switcher.get().readBinary() & 0xFFL) << 24)
             + ((switcher.get().readBinary() & 0xFFL) << 32)
             + ((switcher.get().readBinary() & 0xFFL) << 40)
             + ((switcher.get().readBinary() & 0xFFL) << 48)
             + ((switcher.get().readBinary() & 0xFFL) << 56);
        // @formatter:on
    }

    /**
     * 读取布尔值
     * 
     * @return 读取的布尔值
     * @throws IOException 如果读取失败
     */
    public boolean readBoolean() throws IOException {
        return (switcher.get().readBinary() != 0);
    }

    /**
     * 读取字节数组
     * 
     * @return 读取的字节数组
     * @throws IOException 如果读取失败
     */
    public byte[] readBytesBinary() throws IOException {
        byte[] data = new byte[(int) readVarInt()];
        switcher.get().readBinary(data);
        return data;
    }

    /**
     * 读取UTF-8编码的字符串
     * 
     * @return 读取的字符串
     * @throws IOException 如果读取失败
     */
    public String readUTF8StringBinary() throws IOException {
        byte[] data = new byte[(int) readVarInt()];
        return switcher.get().readBinary(data) > 0 ? new String(data, StandardCharsets.UTF_8) : "";
    }

    /**
     * 读取单个字节
     * 
     * @return 读取的字节
     * @throws IOException 如果读取失败
     */
    public byte readByte() throws IOException {
        return (byte) switcher.get().readBinary();
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
     * 禁用压缩
     */
    public void maybeDisableCompressed() {
        if (enableCompress) {
            switcher.select(false);
        }
    }

    /**
     * 读取浮点数
     * 
     * @return 读取的浮点数
     * @throws IOException 如果读取失败
     */
    @SuppressWarnings("PointlessBitwiseExpression")
    public float readFloat() throws IOException {
        // @formatter:off
        int i = ((switcher.get().readBinary() & 0xFF) << 0)
              + ((switcher.get().readBinary() & 0xFF) << 8)
              + ((switcher.get().readBinary() & 0xFF) << 16)
              + ((switcher.get().readBinary() & 0xFF) << 24);
        // @formatter:on
        return Float.intBitsToFloat(i);
    }

    /**
     * 读取双精度浮点数
     * 
     * @return 读取的双精度浮点数
     * @throws IOException 如果读取失败
     */
    @SuppressWarnings("PointlessBitwiseExpression")
    public double readDouble() throws IOException {
        // @formatter:off
        long l = ((switcher.get().readBinary() & 0xFFL) << 0)
               + ((switcher.get().readBinary() & 0xFFL) << 8)
               + ((switcher.get().readBinary() & 0xFFL) << 16)
               + ((switcher.get().readBinary() & 0xFFL) << 24)
               + ((switcher.get().readBinary() & 0xFFL) << 32)
               + ((switcher.get().readBinary() & 0xFFL) << 40)
               + ((switcher.get().readBinary() & 0xFFL) << 48)
               + ((switcher.get().readBinary() & 0xFFL) << 56);
        // @formatter:on
        return Double.longBitsToDouble(l);
    }

    /**
     * 读取指定大小的字节数组
     * 
     * @param size 要读取的字节数
     * @return 读取的字节数组
     * @throws IOException 如果读取失败
     */
    public byte[] readBytes(int size) throws IOException {
        byte[] bytes = new byte[size];
        switcher.get().readBinary(bytes);
        return bytes;
    }
}
