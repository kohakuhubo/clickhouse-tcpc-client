package com.berry.clickhouse.tcp.client.util;

import com.berry.clickhouse.tcp.client.buffer.BuffedReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BinarySerializerUtil {

    public static byte[] serializeString(String str) {
        byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
        byte[] strLengthBytes = serializeVarInt(strBytes.length);
        byte[] bytes = new byte[strBytes.length + strLengthBytes.length];
        System.arraycopy(strLengthBytes, 0, bytes, 0, strLengthBytes.length);
        System.arraycopy(strBytes, 0, bytes, strLengthBytes.length, strBytes.length);
        return bytes;
    }

    public static byte[] serializeVarInt(int x) {
        byte b1 = (byte) (x & 127);
        byte b2 = (byte) ((x >>> 7) & 127);
        byte b3 = (byte) ((x >>> 14) & 127);
        byte b4 = (byte) ((x >>> 21) & 127);
        byte b5 = (byte) ((x >>> 28) & 127);
        if (b5 != 0) {
            return new byte[]{(byte) (b1 | 128), (byte) (b2 | 128), (byte) (b3 | 128), (byte) (b4 | 128), b5};
        }
        if (b4 != 0) {
            return new byte[]{(byte) (b1 | 128), (byte) (b2 | 128), (byte) (b3 | 128), b4};
        }
        if (b3 != 0) {
            return new byte[]{(byte) (b1 | 128), (byte) (b2 | 128), b3};
        }
        if (b2 != 0) {
            return new byte[]{(byte) (b1 | 128), b2};
        }
        return new byte[]{b1};
    }

    public static int deserializeVarInt(byte[] bytes, int length) {
        int number = 0;
        for (int i = 0; i < length; i++) {
            int byt = bytes[i];
            number |= (byt & 0x7F) << (7 * i);
            if ((byt & 0x8F) == 0) {
                break;
            }
        }
        return number;
    }

    public static int deserializeVarInt(BuffedReader reader, int length) throws IOException {
        int number = 0;
        for (int i = 0; i < length; i++) {
            int byt = reader.readBinary();
            number |= (byt & 0x7F) << (7 * i);
            if ((byt & 0x8F) == 0) {
                break;
            }
        }
        return number;
    }

    public static byte[] writeShort(short i) {
        byte[] bytes = new byte[2];
        bytes[0] = (byte) (i & 0xFF);
        bytes[1] = (byte) ((i >> 8) & 0xFF);
        return bytes;
    }

    public static byte[] writeInt(int i) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (i & 0xFF);
        bytes[1] = (byte) ((i >> 8) & 0xFF);
        bytes[2] = (byte) ((i >> 16) & 0xFF);
        bytes[3] = (byte) ((i >> 24) & 0xFF);
        return bytes;
    }

    public static byte[] writeLong(long i) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) (i & 0xFF);
        bytes[1] = (byte) ((i >> 8) & 0xFF);
        bytes[2] = (byte) ((i >> 16) & 0xFF);
        bytes[3] = (byte) ((i >> 24) & 0xFF);
        bytes[4] = (byte) ((i >> 32) & 0xFF);
        bytes[5] = (byte) ((i >> 40) & 0xFF);
        bytes[6] = (byte) ((i >> 48) & 0xFF);
        bytes[7] = (byte) ((i >> 56) & 0xFF);
        return bytes;
    }

    public static byte[] writeFloat(float num) {
        int x = Float.floatToIntBits(num);
        return writeInt(x);
    }

    public static byte[] writeDouble(float num) {
        long x = Double.doubleToLongBits(num);
        return writeLong(x);
    }

    public static int readByte(byte[] bytes, int offset) {
        return bytes[offset];
    }

    public static int readShort(byte[] bytes, int offset) {
        if (bytes.length != 2) {
            return readByte(bytes, offset);
        } else {
            return (short) (((bytes[offset + 1] & 0xFF) << 8) | (bytes[offset] & 0xFF));
        }
    }

    public static int readInt(byte[] bytes, int offset) {
        if (bytes.length != 4) {
            return readShort(bytes, offset);
        } else {
            return (((bytes[offset + 3] & 0xFF) << 24)
                    | ((bytes[offset + 2] & 0xFF) << 16)
                    | ((bytes[offset + 1] & 0xFF) << 8)
                    | (bytes[offset] & 0xFF));
        }
    }

    public static long readLong(byte[] bytes, int offset) {
        if (bytes.length != 8) {
            return readInt(bytes, offset);
        } else {
            return ((((long) bytes[offset + 7] & 0xFF) << 56)
                    | (((long) bytes[offset + 6] & 0xFF) << 48)
                    | (((long) bytes[offset + 5] & 0xFF) << 40)
                    | (((long) bytes[offset + 4] & 0xFF) << 32)
                    | (((long) bytes[offset + 3] & 0xFF) << 24)
                    | (((long) bytes[offset + 2] & 0xFF) << 16)
                    | (((long) bytes[offset + 1] & 0xFF) << 8)
                    | ((long) bytes[offset] & 0xFF));
        }
    }
}
