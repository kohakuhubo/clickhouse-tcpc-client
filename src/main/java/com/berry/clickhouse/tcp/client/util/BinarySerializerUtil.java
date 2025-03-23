package com.berry.clickhouse.tcp.client.util;

import com.berry.clickhouse.tcp.client.buffer.BuffedReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * BinarySerializerUtil类提供了一系列用于序列化和反序列化基本数据类型的工具方法
 * 包括字符串、整数、长整型、浮点数等的序列化和反序列化
 */
public class BinarySerializerUtil {

    /**
     * 将字符串序列化为字节数组
     * 
     * @param str 要序列化的字符串
     * @return 序列化后的字节数组
     */
    public static byte[] serializeString(String str) {
        byte[] strBytes = str.getBytes(StandardCharsets.UTF_8); // 将字符串转换为字节数组
        byte[] strLengthBytes = serializeVarInt(strBytes.length); // 序列化字符串长度
        byte[] bytes = new byte[strBytes.length + strLengthBytes.length]; // 创建新的字节数组
        System.arraycopy(strLengthBytes, 0, bytes, 0, strLengthBytes.length); // 复制长度字节
        System.arraycopy(strBytes, 0, bytes, strLengthBytes.length, strBytes.length); // 复制字符串字节
        return bytes; // 返回完整的字节数组
    }

    /**
     * 将整数序列化为可变长度字节数组
     * 
     * @param x 要序列化的整数
     * @return 序列化后的字节数组
     */
    public static byte[] serializeVarInt(int x) {
        byte b1 = (byte) (x & 127); // 获取最低7位
        byte b2 = (byte) ((x >>> 7) & 127); // 获取第8到14位
        byte b3 = (byte) ((x >>> 14) & 127); // 获取第15到21位
        byte b4 = (byte) ((x >>> 21) & 127); // 获取第22到28位
        byte b5 = (byte) ((x >>> 28) & 127); // 获取第29到32位
        if (b5 != 0) {
            return new byte[]{(byte) (b1 | 128), (byte) (b2 | 128), (byte) (b3 | 128), (byte) (b4 | 128), b5}; // 返回5字节
        }
        if (b4 != 0) {
            return new byte[]{(byte) (b1 | 128), (byte) (b2 | 128), (byte) (b3 | 128), b4}; // 返回4字节
        }
        if (b3 != 0) {
            return new byte[]{(byte) (b1 | 128), (byte) (b2 | 128), b3}; // 返回3字节
        }
        if (b2 != 0) {
            return new byte[]{(byte) (b1 | 128), b2}; // 返回2字节
        }
        return new byte[]{b1}; // 返回1字节
    }

    /**
     * 从字节数组中反序列化可变长度整数
     * 
     * @param bytes 字节数组
     * @param length 要读取的字节数
     * @return 反序列化后的整数
     */
    public static int deserializeVarInt(byte[] bytes, int length) {
        int number = 0; // 初始化结果
        for (int i = 0; i < length; i++) {
            int byt = bytes[i]; // 获取当前字节
            number |= (byt & 0x7F) << (7 * i); // 反序列化
            if ((byt & 0x80) == 0) { // 检查是否为最后一个字节
                break;
            }
        }
        return number; // 返回反序列化后的整数
    }

    /**
     * 从BuffedReader中反序列化可变长度整数
     * 
     * @param reader BuffedReader实例
     * @param length 要读取的字节数
     * @return 反序列化后的整数
     * @throws IOException IO异常
     */
    public static int deserializeVarInt(BuffedReader reader, int length) throws IOException {
        int number = 0; // 初始化结果
        for (int i = 0; i < length; i++) {
            int byt = reader.readBinary(); // 从reader中读取字节
            number |= (byt & 0x7F) << (7 * i); // 反序列化
            if ((byt & 0x80) == 0) { // 检查是否为最后一个字节
                break;
            }
        }
        return number; // 返回反序列化后的整数
    }

    /**
     * 将短整型序列化为字节数组
     * 
     * @param i 要序列化的短整型
     * @return 序列化后的字节数组
     */
    public static byte[] writeShort(short i) {
        byte[] bytes = new byte[2]; // 创建2字节数组
        bytes[0] = (byte) (i & 0xFF); // 低字节
        bytes[1] = (byte) ((i >> 8) & 0xFF); // 高字节
        return bytes; // 返回字节数组
    }

    /**
     * 将整型序列化为字节数组
     * 
     * @param i 要序列化的整型
     * @return 序列化后的字节数组
     */
    public static byte[] writeInt(int i) {
        byte[] bytes = new byte[4]; // 创建4字节数组
        bytes[0] = (byte) (i & 0xFF); // 低字节
        bytes[1] = (byte) ((i >> 8) & 0xFF); // 第2字节
        bytes[2] = (byte) ((i >> 16) & 0xFF); // 第3字节
        bytes[3] = (byte) ((i >> 24) & 0xFF); // 高字节
        return bytes; // 返回字节数组
    }

    /**
     * 将长整型序列化为字节数组
     * 
     * @param i 要序列化的长整型
     * @return 序列化后的字节数组
     */
    public static byte[] writeLong(long i) {
        byte[] bytes = new byte[8]; // 创建8字节数组
        bytes[0] = (byte) (i & 0xFF); // 低字节
        bytes[1] = (byte) ((i >> 8) & 0xFF); // 第2字节
        bytes[2] = (byte) ((i >> 16) & 0xFF); // 第3字节
        bytes[3] = (byte) ((i >> 24) & 0xFF); // 第4字节
        bytes[4] = (byte) ((i >> 32) & 0xFF); // 第5字节
        bytes[5] = (byte) ((i >> 40) & 0xFF); // 第6字节
        bytes[6] = (byte) ((i >> 48) & 0xFF); // 第7字节
        bytes[7] = (byte) ((i >> 56) & 0xFF); // 高字节
        return bytes; // 返回字节数组
    }

    /**
     * 将浮点数序列化为字节数组
     * 
     * @param num 要序列化的浮点数
     * @return 序列化后的字节数组
     */
    public static byte[] writeFloat(float num) {
        int x = Float.floatToIntBits(num); // 将浮点数转换为整型
        return writeInt(x); // 返回序列化后的字节数组
    }

    /**
     * 将双精度浮点数序列化为字节数组
     * 
     * @param num 要序列化的双精度浮点数
     * @return 序列化后的字节数组
     */
    public static byte[] writeDouble(float num) {
        long x = Double.doubleToLongBits(num); // 将双精度浮点数转换为长整型
        return writeLong(x); // 返回序列化后的字节数组
    }

    /**
     * 从字节数组中读取一个字节
     * 
     * @param bytes 字节数组
     * @param offset 偏移量
     * @return 读取的字节
     */
    public static int readByte(byte[] bytes, int offset) {
        return bytes[offset]; // 返回指定偏移量的字节
    }

    /**
     * 从字节数组中读取一个短整型
     * 
     * @param bytes 字节数组
     * @param offset 偏移量
     * @return 读取的短整型
     */
    public static int readShort(byte[] bytes, int offset) {
        if (bytes.length != 2) {
            return readByte(bytes, offset); // 如果长度不为2，返回字节
        } else {
            return (short) (((bytes[offset + 1] & 0xFF) << 8) | (bytes[offset] & 0xFF)); // 返回短整型
        }
    }

    /**
     * 从字节数组中读取一个整型
     * 
     * @param bytes 字节数组
     * @param offset 偏移量
     * @return 读取的整型
     */
    public static int readInt(byte[] bytes, int offset) {
        if (bytes.length != 4) {
            return readShort(bytes, offset); // 如果长度不为4，返回短整型
        } else {
            return (((bytes[offset + 3] & 0xFF) << 24)
                    | ((bytes[offset + 2] & 0xFF) << 16)
                    | ((bytes[offset + 1] & 0xFF) << 8)
                    | (bytes[offset] & 0xFF)); // 返回整型
        }
    }

    /**
     * 从字节数组中读取一个长整型
     * 
     * @param bytes 字节数组
     * @param offset 偏移量
     * @return 读取的长整型
     */
    public static long readLong(byte[] bytes, int offset) {
        if (bytes.length != 8) {
            return readInt(bytes, offset); // 如果长度不为8，返回整型
        } else {
            return ((((long) bytes[offset + 7] & 0xFF) << 56)
                    | (((long) bytes[offset + 6] & 0xFF) << 48)
                    | (((long) bytes[offset + 5] & 0xFF) << 40)
                    | (((long) bytes[offset + 4] & 0xFF) << 32)
                    | (((long) bytes[offset + 3] & 0xFF) << 24)
                    | (((long) bytes[offset + 2] & 0xFF) << 16)
                    | (((long) bytes[offset + 1] & 0xFF) << 8)
                    | ((long) bytes[offset] & 0xFF)); // 返回长整型
        }
    }
}
