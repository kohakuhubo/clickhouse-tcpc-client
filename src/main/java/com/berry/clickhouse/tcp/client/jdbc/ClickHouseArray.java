package com.berry.clickhouse.tcp.client.jdbc;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.log.Logger;
import com.berry.clickhouse.tcp.client.log.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.function.BiFunction;

public class ClickHouseArray {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseArray.class);

    private final IDataType<?> elementDataType;
    private final Object[] elements;

    public ClickHouseArray(IDataType<?> elementDataType, Object[] elements) {
        this.elementDataType = elementDataType;
        this.elements = elements;
    }
    public String getBaseTypeName() throws SQLException {
        return elementDataType.name();
    }

    public Object[] getArray() throws SQLException {
        return elements;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",", "[", "]");
        for (Object item : elements) {
            joiner.add(String.valueOf(item));
        }
        return joiner.toString();
    }

    public ClickHouseArray slice(int offset, int length) {
        Object[] result = new Object[length];
        if (length >= 0) System.arraycopy(elements, offset, result, 0, length);
        return new ClickHouseArray(elementDataType, result);
    }

    public ClickHouseArray mapElements(BiFunction<IDataType<?>, Object, Object> mapFunc) {
        Object[] mapped = Arrays.stream(elements).map(elem -> mapFunc.apply(elementDataType, elem)).toArray();
        return new ClickHouseArray(elementDataType, mapped);
    }
}
