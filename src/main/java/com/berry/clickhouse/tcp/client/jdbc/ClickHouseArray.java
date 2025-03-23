package com.berry.clickhouse.tcp.client.jdbc;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.log.Logger;
import com.berry.clickhouse.tcp.client.log.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.function.BiFunction;

/**
 * ClickHouseArray类表示ClickHouse中的数组类型
 * 提供对数组元素的访问和操作
 */
public class ClickHouseArray {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseArray.class);

    private final IDataType<?> elementDataType; // 数组元素的数据类型
    private final Object[] elements; // 数组元素

    /**
     * 构造函数，初始化ClickHouseArray实例
     * 
     * @param elementDataType 数组元素的数据类型
     * @param elements 数组元素
     */
    public ClickHouseArray(IDataType<?> elementDataType, Object[] elements) {
        this.elementDataType = elementDataType;
        this.elements = elements;
    }

    /**
     * 获取数组的基本类型名称
     * 
     * @return 数组元素的类型名称
     * @throws SQLException 如果获取类型名称时发生错误
     */
    public String getBaseTypeName() throws SQLException {
        return elementDataType.name();
    }

    /**
     * 获取数组元素
     * 
     * @return 数组元素
     * @throws SQLException 如果获取元素时发生错误
     */
    public Object[] getArray() throws SQLException {
        return elements;
    }

    /**
     * 返回数组的字符串表示
     * 
     * @return 包含数组元素的字符串
     */
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",", "[", "]");
        for (Object item : elements) {
            joiner.add(String.valueOf(item));
        }
        return joiner.toString();
    }

    /**
     * 切片数组，返回指定范围的子数组
     * 
     * @param offset 起始索引
     * @param length 子数组的长度
     * @return ClickHouseArray的子数组
     */
    public ClickHouseArray slice(int offset, int length) {
        Object[] result = new Object[length];
        if (length >= 0) System.arraycopy(elements, offset, result, 0, length);
        return new ClickHouseArray(elementDataType, result);
    }

    /**
     * 映射数组元素，返回新的ClickHouseArray
     * 
     * @param mapFunc 映射函数
     * @return 映射后的ClickHouseArray
     */
    public ClickHouseArray mapElements(BiFunction<IDataType<?>, Object, Object> mapFunc) {
        Object[] mapped = Arrays.stream(elements).map(elem -> mapFunc.apply(elementDataType, elem)).toArray();
        return new ClickHouseArray(elementDataType, mapped);
    }
}
