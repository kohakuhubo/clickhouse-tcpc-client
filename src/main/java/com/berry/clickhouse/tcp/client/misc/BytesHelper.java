package com.berry.clickhouse.tcp.client.misc;

/**
 * BytesHelper接口提供了一些用于字节操作的默认方法
 * 包括字节数组的获取和设置
 */
public interface BytesHelper {

    /**
     * 获取整数的字节数组
     * 
     * @param i 整数
     * @return 字节数组
     */
    default byte[] getBytes(int i) {
        byte[] memory = new byte[Integer.BYTES];
        setInt(memory, 0, i);
        return memory; // 返回整数的字节数组
    }

    default byte[] getBytesLE(int i) {
        byte[] memory = new byte[Integer.BYTES];
        setIntLE(memory, 0, i);
        return memory;
    }

    default byte[] getBytes(long l) {
        byte[] memory = new byte[Long.BYTES];
        setLong(memory, 0, l);
        return memory;
    }

    default byte[] getBytesLE(long l) {
        byte[] memory = new byte[Long.BYTES];
        setLongLE(memory, 0, l);
        return memory;
    }

    default byte[] getBytes(long[] longArray) {
        byte[] memory = new byte[Long.BYTES * longArray.length];
        for (int index = 0; index < longArray.length; index++) {
            long l = longArray[index];
            setLong(memory, index * Long.BYTES, l);
        }
        return memory;
    }

    default byte getByte(byte[] memory, int index) {
        return memory[index];
    }

    default short getShort(byte[] memory, int index) {
        return (short) (memory[index] << 8 | memory[index + 1] & 0xFF);
    }

    default short getShortLE(byte[] memory, int index) {
        return (short) (memory[index] & 0xff | memory[index + 1] << 8);
    }

    default int getUnsignedMedium(byte[] memory, int index) {
        // @formatter:off
        return (memory[index]     & 0xff) << 16 |
               (memory[index + 1] & 0xff) <<  8 |
                memory[index + 2] & 0xff;
        // @formatter:on
    }

    default int getUnsignedMediumLE(byte[] memory, int index) {
        // @formatter:off
        return memory[index]     & 0xff        |
              (memory[index + 1] & 0xff) <<  8 |
              (memory[index + 2] & 0xff) << 16;
        // @formatter:on
    }

    default int getInt(byte[] memory, int index) {
        // @formatter:off
        return (memory[index]     & 0xff) << 24 |
               (memory[index + 1] & 0xff) << 16 |
               (memory[index + 2] & 0xff) <<  8 |
                memory[index + 3] & 0xff;
        // @formatter:on
    }

    default int getIntLE(byte[] memory, int index) {
        // @formatter:off
        return  memory[index]      & 0xff       |
               (memory[index + 1] & 0xff) << 8  |
               (memory[index + 2] & 0xff) << 16 |
               (memory[index + 3] & 0xff) << 24;
        // @formatter:on
    }

    default long getLong(byte[] memory, int index) {
        // @formatter:off
        return ((long) memory[index]     & 0xff) << 56 |
               ((long) memory[index + 1] & 0xff) << 48 |
               ((long) memory[index + 2] & 0xff) << 40 |
               ((long) memory[index + 3] & 0xff) << 32 |
               ((long) memory[index + 4] & 0xff) << 24 |
               ((long) memory[index + 5] & 0xff) << 16 |
               ((long) memory[index + 6] & 0xff) <<  8 |
                (long) memory[index + 7] & 0xff;
        // @formatter:on
    }

    default long getLongLE(byte[] memory, int index) {
        // @formatter:off
        return (long) memory[index]      & 0xff       |
              ((long) memory[index + 1] & 0xff) <<  8 |
              ((long) memory[index + 2] & 0xff) << 16 |
              ((long) memory[index + 3] & 0xff) << 24 |
              ((long) memory[index + 4] & 0xff) << 32 |
              ((long) memory[index + 5] & 0xff) << 40 |
              ((long) memory[index + 6] & 0xff) << 48 |
              ((long) memory[index + 7] & 0xff) << 56;
        // @formatter:on
    }

    default void setByte(byte[] memory, int index, int value) {
        memory[index] = (byte) value;
    }

    default void setShort(byte[] memory, int index, int value) {
        // @formatter:off
        memory[index]     = (byte) (value >>> 8);
        memory[index + 1] = (byte) value;
        // @formatter:on
    }

    default void setShortLE(byte[] memory, int index, int value) {
        // @formatter:off
        memory[index]     = (byte) value;
        memory[index + 1] = (byte) (value >>> 8);
        // @formatter:on
    }

    default void setMedium(byte[] memory, int index, int value) {
        // @formatter:off
        memory[index]     = (byte) (value >>> 16);
        memory[index + 1] = (byte) (value >>> 8);
        memory[index + 2] = (byte) value;
        // @formatter:on
    }

    default void setMediumLE(byte[] memory, int index, int value) {
        // @formatter:off
        memory[index]     = (byte) value;
        memory[index + 1] = (byte) (value >>> 8);
        memory[index + 2] = (byte) (value >>> 16);
        // @formatter:on
    }

    default void setInt(byte[] memory, int index, int value) {
        // @formatter:off
        memory[index]     = (byte) (value >>> 24);
        memory[index + 1] = (byte) (value >>> 16);
        memory[index + 2] = (byte) (value >>> 8);
        memory[index + 3] = (byte) value;
        // @formatter:on
    }

    default void setIntLE(byte[] memory, int index, int value) {
        // @formatter:off
        memory[index]     = (byte) value;
        memory[index + 1] = (byte) (value >>> 8);
        memory[index + 2] = (byte) (value >>> 16);
        memory[index + 3] = (byte) (value >>> 24);
        // @formatter:on
    }

    default void setLong(byte[] memory, int index, long value) {
        // @formatter:off
        memory[index]     = (byte) (value >>> 56);
        memory[index + 1] = (byte) (value >>> 48);
        memory[index + 2] = (byte) (value >>> 40);
        memory[index + 3] = (byte) (value >>> 32);
        memory[index + 4] = (byte) (value >>> 24);
        memory[index + 5] = (byte) (value >>> 16);
        memory[index + 6] = (byte) (value >>> 8);
        memory[index + 7] = (byte) value;
        // @formatter:on
    }

    default void setLongLE(byte[] memory, int index, long value) {
        // @formatter:off
        memory[index]     = (byte) value;
        memory[index + 1] = (byte) (value >>> 8);
        memory[index + 2] = (byte) (value >>> 16);
        memory[index + 3] = (byte) (value >>> 24);
        memory[index + 4] = (byte) (value >>> 32);
        memory[index + 5] = (byte) (value >>> 40);
        memory[index + 6] = (byte) (value >>> 48);
        memory[index + 7] = (byte) (value >>> 56);
        // @formatter:on
    }
}
