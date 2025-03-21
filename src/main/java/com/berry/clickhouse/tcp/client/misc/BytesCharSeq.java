package com.berry.clickhouse.tcp.client.misc;

public class BytesCharSeq implements CharSequence {

    private final byte[] bytes;

    public BytesCharSeq(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public int length() {
        return bytes.length;
    }

    @Override
    public char charAt(int index) {
        return (char) bytes[index];
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        byte[] newBytes = new byte[end - start];
        System.arraycopy(bytes, start, newBytes, 0, end - start);
        return new BytesCharSeq(newBytes);
    }

    @Override
    public String toString() {
        return "BytesCharSeq, length: " + length();
    }

    public byte[] bytes() {
        return bytes;
    }
}
