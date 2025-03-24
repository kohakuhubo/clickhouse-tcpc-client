package com.berry.clickhouse.tcp.client.util;

public class ByteConverter {

    // 将 int 转换为 4 字节数组
    public static byte[] intToByteArray(int value, boolean isLittleEndian) {
        byte[] byteArray = new byte[4];
        if (isLittleEndian) {
            byteArray[0] = (byte) (value & 0xFF);
            byteArray[1] = (byte) ((value >> 8) & 0xFF);
            byteArray[2] = (byte) ((value >> 16) & 0xFF);
            byteArray[3] = (byte) ((value >> 24) & 0xFF);
        } else {
            byteArray[0] = (byte) ((value >> 24) & 0xFF);
            byteArray[1] = (byte) ((value >> 16) & 0xFF);
            byteArray[2] = (byte) ((value >> 8) & 0xFF);
            byteArray[3] = (byte) (value & 0xFF);
        }
        return byteArray;
    }

    // 将 short 转换为 2 字节数组
    public static byte[] shortToByteArray(short value, boolean isLittleEndian) {
        byte[] byteArray = new byte[2];
        if (isLittleEndian) {
            byteArray[0] = (byte) (value & 0xFF);
            byteArray[1] = (byte) ((value >> 8) & 0xFF);
        } else {
            byteArray[0] = (byte) ((value >> 8) & 0xFF);
            byteArray[1] = (byte) (value & 0xFF);
        }
        return byteArray;
    }

    // 将 long 转换为 8 字节数组
    public static byte[] longToByteArray(long value, boolean isLittleEndian) {
        byte[] byteArray = new byte[8];
        if (isLittleEndian) {
            for (int i = 0; i < 8; i++) {
                byteArray[i] = (byte) (value >> (i * 8));
            }
        } else {
            for (int i = 0; i < 8; i++) {
                byteArray[7 - i] = (byte) (value >> (i * 8));
            }
        }
        return byteArray;
    }

    // 将 float 转换为 4 字节数组
    public static byte[] floatToByteArray(float value, boolean isLittleEndian) {
        return intToByteArray(Float.floatToIntBits(value), isLittleEndian);
    }

    // 将 double 转换为 8 字节数组
    public static byte[] doubleToByteArray(double value, boolean isLittleEndian) {
        return longToByteArray(Double.doubleToLongBits(value), isLittleEndian);
    }

    // 从字节数组转换为 int
    public static int byteArrayToInt(byte[] byteArray, boolean isLittleEndian) {
        if (byteArray.length < 4) {
            throw new IllegalArgumentException("Byte array must be at least 4 bytes long.");
        }
        int result = 0;
        if (isLittleEndian) {
            for (int i = 0; i < 4; i++) {
                result |= (byteArray[i] & 0xFF) << (i * 8);
            }
        } else {
            for (int i = 0; i < 4; i++) {
                result |= (byteArray[3 - i] & 0xFF) << (i * 8);
            }
        }
        return result;
    }

    // 从字节数组转换为 short
    public static short byteArrayToShort(byte[] byteArray, boolean isLittleEndian) {
        if (byteArray.length < 2) {
            throw new IllegalArgumentException("Byte array must be at least 2 bytes long.");
        }
        short result = 0;
        if (isLittleEndian) {
            result |= (byteArray[0] & 0xFF);
            result |= (byteArray[1] & 0xFF) << 8;
        } else {
            result |= (byteArray[1] & 0xFF);
            result |= (byteArray[0] & 0xFF) << 8;
        }
        return result;
    }

    // 从字节数组转换为 long
    public static long byteArrayToLong(byte[] byteArray, boolean isLittleEndian) {
        if (byteArray.length < 8) {
            throw new IllegalArgumentException("Byte array must be at least 8 bytes long.");
        }
        long result = 0;
        if (isLittleEndian) {
            for (int i = 0; i < 8; i++) {
                result |= ((long) (byteArray[i] & 0xFF)) << (i * 8);
            }
        } else {
            for (int i = 0; i < 8; i++) {
                result |= ((long) (byteArray[7 - i] & 0xFF)) << (i * 8);
            }
        }
        return result;
    }

    // 从字节数组转换为 float
    public static float byteArrayToFloat(byte[] byteArray, boolean isLittleEndian) {
        return Float.intBitsToFloat(byteArrayToInt(byteArray, isLittleEndian));
    }

    // 从字节数组转换为 double
    public static double byteArrayToDouble(byte[] byteArray, boolean isLittleEndian) {
        return Double.longBitsToDouble(byteArrayToLong(byteArray, isLittleEndian));
    }
}
