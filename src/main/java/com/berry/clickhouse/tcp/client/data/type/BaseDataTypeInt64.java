package com.berry.clickhouse.tcp.client.data.type;

import java.math.BigInteger;


public interface BaseDataTypeInt64<CK> extends BaseDataTypeInt<CK> {

    default BigInteger parseBigIntegerPositive(String num, int bitlen) {
        BigInteger b = new BigInteger(num);
        if (b.compareTo(BigInteger.ZERO) < 0) {
            b = b.add(BigInteger.ONE.shiftLeft(bitlen));
        }
        return b;
    }
}
