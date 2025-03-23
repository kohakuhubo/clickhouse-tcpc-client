/**
 * ClickHouse整数类型的基础接口
 * 所有整数类型（Int8、Int16、Int32、Int64、UInt8、UInt16、UInt32、UInt64）的共同基础接口
 */
package com.berry.clickhouse.tcp.client.data.type;

import com.berry.clickhouse.tcp.client.data.IDataType;

/**
 * 整数类型基础接口
 * 为所有整数数据类型提供共同的基础功能
 * 
 * @param <CK> 整数类型对应的Java类型
 */
public interface BaseDataTypeInt<CK> extends IDataType<CK> {

}
