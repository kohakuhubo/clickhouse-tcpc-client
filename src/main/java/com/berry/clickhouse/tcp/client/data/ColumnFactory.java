package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeArray;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeMap;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeNullable;
import com.berry.clickhouse.tcp.client.data.type.complex.DataTypeTuple;

public class ColumnFactory {

    public static IColumn createColumn(String name, IDataType<?> type, byte[] nameBytes, Object[] values) {
        if (type instanceof DataTypeArray)
            return new ColumnArray(name, (DataTypeArray) type, nameBytes, values);

        if (type instanceof DataTypeNullable)
            return new ColumnNullable(name, (DataTypeNullable) type, nameBytes, values);

        if (type instanceof DataTypeTuple)
            return new ColumnTuple(name, (DataTypeTuple) type, nameBytes, values);
        
        if (type instanceof DataTypeMap)
           return new ColumnMap(name, (DataTypeMap) type, nameBytes, values);

        return new Column(name, type, nameBytes, values);
    }
}
