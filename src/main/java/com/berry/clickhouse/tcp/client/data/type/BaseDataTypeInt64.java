package com.berry.clickhouse.tcp.client.data.type;

import java.math.BigInteger;

/**
 * ClickHouse 64位整数类型的基础接口
 * 用于Int64和UInt64数据类型的共同基础接口
 */
public interface BaseDataTypeInt64<CK> extends BaseDataTypeInt<CK> {

    /**
     * 将字符串解析为正的BigInteger
     * 
     * @param num 字符串表示的数字
     * @param bitlen 位数
     * @return 解析后的BigInteger
     */
    default BigInteger parseBigIntegerPositive(String num, int bitlen) {
        BigInteger b = new BigInteger(num);
        if (b.compareTo(BigInteger.ZERO) < 0) {
            b = b.add(BigInteger.ONE.shiftLeft(bitlen));
        }
        return b;
    }

    @Override
    default int byteSize() {
        return Long.BYTES;
    }
}
