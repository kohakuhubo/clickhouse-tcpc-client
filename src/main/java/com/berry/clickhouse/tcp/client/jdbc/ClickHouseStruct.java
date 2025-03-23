package com.berry.clickhouse.tcp.client.jdbc;

import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.log.Logger;
import com.berry.clickhouse.tcp.client.log.LoggerFactory;
import com.berry.clickhouse.tcp.client.misc.Validate;

import java.sql.SQLException;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClickHouseStruct类表示ClickHouse中的结构类型
 * 提供对结构属性的访问和操作
 */
public class ClickHouseStruct {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseStruct.class);
    private static final Pattern ATTR_INDEX_REGEX = Pattern.compile("_(\\d+)"); // 属性索引正则表达式

    private final String type; // 结构类型
    private final Object[] attributes; // 结构属性

    /**
     * 构造函数，初始化ClickHouseStruct实例
     * 
     * @param type 结构类型
     * @param attributes 结构属性
     */
    public ClickHouseStruct(String type, Object[] attributes) {
        this.type = type;
        this.attributes = attributes;
    }

    /**
     * 获取结构的SQL类型名称
     * 
     * @return 结构类型名称
     * @throws SQLException 如果获取类型名称时发生错误
     */
    public String getSQLTypeName() throws SQLException {
        return type;
    }

    /**
     * 获取结构的属性
     * 
     * @return 结构属性
     * @throws SQLException 如果获取属性时发生错误
     */
    public Object[] getAttributes() throws SQLException {
        return attributes;
    }

    /**
     * 根据给定的属性名称映射结构属性
     * 
     * @param map 属性名称与类型的映射
     * @return 映射后的属性数组
     * @throws SQLException 如果映射过程中发生错误
     */
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        int i = 0;
        Object[] res = new Object[map.size()];
        for (String attrName : map.keySet()) {
            Class<?> clazz = map.get(attrName);
            Matcher matcher = ATTR_INDEX_REGEX.matcher(attrName);
            Validate.isTrue(matcher.matches(), "Can't find " + attrName + ".");

            int attrIndex = Integer.parseInt(matcher.group(1)) - 1;
            Validate.isTrue(attrIndex < attributes.length, "Can't find " + attrName + ".");
            Validate.isTrue(clazz.isInstance(attributes[attrIndex]),
                    "Can't cast " + attrName + " to " + clazz.getName());

            res[i++] = clazz.cast(attributes[attrIndex]);
        }
        return res;
    }

    /**
     * 返回结构的字符串表示
     * 
     * @return 包含结构属性的字符串
     */
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",", "(", ")");
        for (Object item : attributes) {
            joiner.add(String.valueOf(item));
        }
        return joiner.toString();
    }

    /**
     * 映射结构属性，返回新的ClickHouseStruct
     * 
     * @param nestedTypes 嵌套类型数组
     * @param mapFunc 映射函数
     * @return 映射后的ClickHouseStruct
     */
    public ClickHouseStruct mapAttributes(IDataType<?>[] nestedTypes, BiFunction<IDataType<?>, Object, Object> mapFunc) {
        assert nestedTypes.length == attributes.length;
        Object[] mapped = new Object[attributes.length];
        for (int i = 0; i < attributes.length; i++) {
            mapped[i] = mapFunc.apply(nestedTypes[i], attributes[i]);
        }
        return new ClickHouseStruct(type, mapped);
    }
}
