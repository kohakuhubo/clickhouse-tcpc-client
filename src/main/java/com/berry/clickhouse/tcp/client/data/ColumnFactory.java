package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeArray;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeMap;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeNullable;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeTuple;

/**
 * ColumnFactory类用于创建列对象
 * 根据数据类型返回相应的列实现
 */
public class ColumnFactory {

    /**
     * 创建列对象
     * 
     * @param name 列名
     * @param type 数据类型
     * @param nameBytes 列名字节数组
     * @param values 列值数组
     * @return 创建的列对象
     */
    public static IColumn createColumn(String name, IDataType<?> type, byte[] nameBytes, Object[] values) {
        if (type instanceof DataTypeArray)
            return new ColumnArray(name, (DataTypeArray) type, nameBytes, values); // 创建数组列

        if (type instanceof DataTypeNullable)
            return new ColumnNullable(name, (DataTypeNullable) type, nameBytes, values); // 创建可空列

        if (type instanceof DataTypeTuple)
            return new ColumnTuple(name, (DataTypeTuple) type, nameBytes, values); // 创建元组列
        
        if (type instanceof DataTypeMap)
           return new ColumnMap(name, (DataTypeMap) type, nameBytes, values); // 创建映射列

        return new Column(name, type, nameBytes, values); // 创建普通列
    }
}
