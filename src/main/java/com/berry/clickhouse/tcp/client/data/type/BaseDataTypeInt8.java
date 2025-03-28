package com.berry.clickhouse.tcp.client.data.type;



public interface BaseDataTypeInt8<CK> extends BaseDataTypeInt<CK> {

    @Override
    default int byteSize() {
        return Byte.BYTES;
    }

}
