package com.berry.clickhouse.tcp.client.serde;

import com.berry.clickhouse.tcp.client.buffer.BuffedReader;
import com.berry.clickhouse.tcp.client.buffer.CompressedBuffedReader;
import com.berry.clickhouse.tcp.client.misc.Switcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BinaryDeserializer {

    private final Switcher<BuffedReader> switcher;
    private final boolean enableCompress;

    public BinaryDeserializer(BuffedReader buffedReader, boolean enableCompress) {
        this.enableCompress = enableCompress;
        BuffedReader compressedReader = null;
        if (enableCompress) {
            compressedReader = new CompressedBuffedReader(buffedReader);
        }
        switcher = new Switcher<>(compressedReader, buffedReader);
    }

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

    @SuppressWarnings("PointlessBitwiseExpression")
    public short readShort() throws IOException {
        return (short) (((switcher.get().readBinary() & 0xFF) << 0)
                      + ((switcher.get().readBinary() & 0xFF) << 8));
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public int readInt() throws IOException {
        // @formatter:off
        return ((switcher.get().readBinary() & 0xFF) << 0)
             + ((switcher.get().readBinary() & 0xFF) << 8)
             + ((switcher.get().readBinary() & 0xFF) << 16)
             + ((switcher.get().readBinary() & 0xFF) << 24);
        // @formatter:on
    }

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

    public boolean readBoolean() throws IOException {
        return (switcher.get().readBinary() != 0);
    }

    public byte[] readBytesBinary() throws IOException {
        byte[] data = new byte[(int) readVarInt()];
        switcher.get().readBinary(data);
        return data;
    }

    public String readUTF8StringBinary() throws IOException {
        byte[] data = new byte[(int) readVarInt()];
        return switcher.get().readBinary(data) > 0 ? new String(data, StandardCharsets.UTF_8) : "";
    }

    public byte readByte() throws IOException {
        return (byte) switcher.get().readBinary();
    }

    public void maybeEnableCompressed() {
        if (enableCompress) {
            switcher.select(false);
        }
    }

    public void maybeDisableCompressed() {
        if (enableCompress) {
            switcher.select(true);
        }
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public float readFloat() throws IOException {
        // @formatter:off
        return Float.intBitsToFloat(
               ((switcher.get().readBinary() & 0xFF) << 0)
             + ((switcher.get().readBinary() & 0xFF) << 8)
             + ((switcher.get().readBinary() & 0xFF) << 16)
             + ((switcher.get().readBinary()       ) << 24));
        // @formatter:on
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public double readDouble() throws IOException {
        // @formatter:off
        return Double.longBitsToDouble(
                ((switcher.get().readBinary() & 0xFFL) << 0 )
              + ((switcher.get().readBinary() & 0xFFL) << 8 )
              + ((switcher.get().readBinary() & 0xFFL) << 16)
              + ((switcher.get().readBinary() & 0xFFL) << 24)
              + ((switcher.get().readBinary() & 0xFFL) << 32)
              + ((switcher.get().readBinary() & 0xFFL) << 40)
              + ((switcher.get().readBinary() & 0xFFL) << 48)
              + ((switcher.get().readBinary() & 0xFFL) << 56)
        );
        // @formatter:on
    }

    public byte[] readBytes(int size) throws IOException {
        byte[] bytes = new byte[size];
        switcher.get().readBinary(bytes);
        return bytes;
    }
}
