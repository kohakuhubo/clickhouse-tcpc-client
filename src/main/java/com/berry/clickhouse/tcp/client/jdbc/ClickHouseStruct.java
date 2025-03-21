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

public class ClickHouseStruct {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseStruct.class);
    private static final Pattern ATTR_INDEX_REGEX = Pattern.compile("_(\\d+)");

    private final String type;
    private final Object[] attributes;

    public ClickHouseStruct(String type, Object[] attributes) {
        this.type = type;
        this.attributes = attributes;
    }

    public String getSQLTypeName() throws SQLException {
        return type;
    }

    public Object[] getAttributes() throws SQLException {
        return attributes;
    }

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

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",", "(", ")");
        for (Object item : attributes) {
            joiner.add(String.valueOf(item));
        }
        return joiner.toString();
    }

    public ClickHouseStruct mapAttributes(IDataType<?>[] nestedTypes, BiFunction<IDataType<?>, Object, Object> mapFunc) {
        assert nestedTypes.length == attributes.length;
        Object[] mapped = new Object[attributes.length];
        for (int i = 0; i < attributes.length; i++) {
            mapped[i] = mapFunc.apply(nestedTypes[i], attributes[i]);
        }
        return new ClickHouseStruct(type, mapped);
    }
}
