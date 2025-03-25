/**
 * ClickHouse 32位整数类型的基础接口
 * 用于Int32和UInt32数据类型的共同基础接口
 */
package com.berry.clickhouse.tcp.client.data.type;

/**
 * 32位整数类型基础接口
 * 为32位整数数据类型提供特定功能
 * 
 * @param <CK> 32位整数类型对应的Java类型
 */
public interface BaseDataTypeInt32<CK> extends BaseDataTypeInt<CK> {

    @Override
    default int byteSize() {
        return Integer.BYTES;
    }

}
