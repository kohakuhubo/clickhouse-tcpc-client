package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeArray;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeMap;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeNullable;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeTuple;

public class ColumnFactory {

    public static IColumn createColumn(String name, IDataType<?> type, byte[] nameBytes, Object[] values, boolean useSystemBuffer) {
        if (type instanceof DataTypeArray)
            return new ColumnArray(name, (DataTypeArray) type, nameBytes, values, useSystemBuffer);

        if (type instanceof DataTypeNullable)
            return new ColumnNullable(name, (DataTypeNullable) type, nameBytes, values, useSystemBuffer);

        if (type instanceof DataTypeTuple)
            return new ColumnTuple(name, (DataTypeTuple) type, nameBytes, values, useSystemBuffer);
        
        if (type instanceof DataTypeMap)
           return new ColumnMap(name, (DataTypeMap) type, nameBytes, values, useSystemBuffer);

        return new Column(name, type, nameBytes, values, useSystemBuffer);
    }
}
