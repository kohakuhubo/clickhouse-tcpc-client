/**
 * ClickHouse Map数据类型的实现
 * 用于处理ClickHouse中的Map类型，例如Map(String, Int32)等
 * 提供Map数据的序列化和反序列化功能
 */
package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.data.DataTypeFactory;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.data.type.DataTypeInt64;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Map数据类型实现
 * 处理ClickHouse中的Map类型，包含键值对
 * ClickHouse中的Map实际上以两个数组实现：一个用于键，一个用于值
 */
public class DataTypeMap implements IDataType<Object> {
    /**
     * Map类型创建器
     * 用于根据词法分析结果创建DataTypeMap实例
     */
    public static DataTypeCreator<Object> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        IDataType<?> key = DataTypeFactory.get(lexer, serverContext);
        Validate.isTrue(lexer.character() == ',');
        IDataType<?> value = DataTypeFactory.get(lexer, serverContext);
        Validate.isTrue(lexer.character() == ')');

        IDataType<?>[] nestedTypes = new IDataType[]{key, value};
        String name = "Map(" + key.name() + ", " + value.name() + ")";

        return new DataTypeMap(name, nestedTypes, (DataTypeInt64) DataTypeFactory.get("Int64", serverContext));
    };

    /**
     * 数据类型名称
     */
    private final String name;

    /**
     * 嵌套数据类型数组，index 0为键类型，index 1为值类型
     */
    private final IDataType<?>[] nestedTypes;

    /**
     * 用于存储偏移量的Int64数据类型
     */
    private final DataTypeInt64 offsetIDataType;

    /**
     * 创建Map数据类型
     * 
     * @param name 类型名称，如"Map(String, Int32)"
     * @param nestedTypes 包含键类型和值类型的数组
     * @param offsetIDataType 用于存储偏移量的Int64数据类型
     */
    public DataTypeMap(String name, IDataType<?>[] nestedTypes, DataTypeInt64 offsetIDataType) {
        this.name = name;
        this.nestedTypes = nestedTypes;
        this.offsetIDataType = offsetIDataType;
    }

    /**
     * 获取数据类型名称
     * 
     * @return 数据类型名称，如"Map(String, Int32)"
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * 获取数据类型对应的Java类
     * 
     * @return Object.class (实际上Map被处理为Object)
     */
    @Override
    public Class<Object> javaType() {
        return Object.class;
    }

    /**
     * 将Map序列化为二进制格式
     * 先序列化所有键，再序列化所有值
     * 
     * @param data 要序列化的Map
     * @param serializer 二进制序列化器
     * @throws SQLException 如果序列化过程中发生SQL错误
     * @throws IOException 如果序列化过程中发生I/O错误
     */
    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        if (data instanceof Map) {
            Map<?, ?> dataMap = (Map<?, ?>) data;
            getNestedTypes()[0].serializeBinaryBulk(dataMap.keySet().toArray(), serializer);
            getNestedTypes()[1].serializeBinaryBulk(dataMap.values().toArray(), serializer);
        }
    }

    /**
     * 批量将Map序列化为二进制格式
     * 先写入Map数量，然后序列化所有键，再序列化所有值
     * 
     * @param data 要序列化的Map数组
     * @param serializer 二进制序列化器
     * @throws SQLException 如果序列化过程中发生SQL错误
     * @throws IOException 如果序列化过程中发生I/O错误
     */
    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        offsetIDataType.serializeBinary((long) data.length, serializer);
        // 先序列化所有Map的键
        for (Object obj : data) {
            if (obj instanceof Map) {
                Map<?, ?> dataMap = (Map<?, ?>) obj;
                getNestedTypes()[0].serializeBinaryBulk(dataMap.keySet().toArray(), serializer);
            }
        }
        // 再序列化所有Map的值
        for (Object obj : data) {
            if (obj instanceof Map) {
                Map<?, ?> dataMap = (Map<?, ?>) obj;
                getNestedTypes()[1].serializeBinaryBulk(dataMap.values().toArray(), serializer);
            }
        }
    }

    /**
     * 从SQL词法分析器解析Map
     * 解析格式为 {key1:value1, key2:value2, ...}
     * 
     * @param lexer SQL词法分析器
     * @return 解析后的Map
     * @throws SQLException 如果解析过程中发生错误
     */
    @Override
    public Object deserializeText(SQLLexer lexer) throws SQLException {
        Map<Object, Object> result = new HashMap<>();
        Object key = null;
        Object value = null;
        Validate.isTrue(lexer.character() == '{');
        for (; ; ) {
            if (lexer.isCharacter('}')) {
                lexer.character();
                break;
            }
            key = getNestedTypes()[0].deserializeText(lexer);
            Validate.isTrue(lexer.character() == ':');
            value = getNestedTypes()[1].deserializeText(lexer);
            if (lexer.isCharacter(',')) {
                lexer.character();
            }
            result.put(key, value);
        }

        return result;
    }

    /**
     * 从二进制流反序列化Map
     * 先读取Map大小，然后读取所有键和所有值，最后组装成Map
     * 
     * @param deserializer 二进制反序列化器
     * @return 反序列化后的Map
     * @throws SQLException 如果反序列化过程中发生SQL错误
     * @throws IOException 如果反序列化过程中发生I/O错误
     */
    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Map<Object, Object> map = new HashMap<Object, Object>();
        Long offset = offsetIDataType.deserializeBinary(deserializer);
        // 读取所有键
        Object[] keys = getNestedTypes()[0].deserializeBinaryBulk(offset.intValue(), deserializer);
        // 读取所有值
        Object[] values = getNestedTypes()[1].deserializeBinaryBulk(offset.intValue(), deserializer);
        // 组装Map
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }

        return map;
    }

    /**
     * 批量从二进制流反序列化Map
     * 读取所有Map的大小，然后读取所有键和所有值，最后按照偏移量组装成多个Map
     * 
     * @param rows 行数
     * @param deserializer 二进制反序列化器
     * @return 反序列化后的Map数组
     * @throws IOException 如果反序列化过程中发生I/O错误
     * @throws SQLException 如果反序列化过程中发生SQL错误
     */
    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        Object[] arrays = new Object[rows];
        if (rows == 0) {
            return arrays;
        }

        // 读取所有行的偏移量
        int[] offsets = Arrays.stream(offsetIDataType.deserializeBinaryBulk(rows, deserializer))
                .mapToInt(value -> ((Long) value).intValue()).toArray();
        // 读取所有键
        Object[] keys = getNestedTypes()[0].deserializeBinaryBulk(offsets[rows - 1], deserializer);
        // 读取所有值
        Object[] values = getNestedTypes()[1].deserializeBinaryBulk(offsets[rows - 1], deserializer);

        // 根据偏移量构建每行的Map
        Map<Object, Object> map = new HashMap<Object, Object>();
        for (int row = 0, lastOffset = 0; row < rows; row++) {
            int offset = offsets[row];
            for (int i = lastOffset; i < offset; i++) {
                map.put(keys[i], values[i]);
            }
            lastOffset = offset;
            arrays[row] = map;
            map = new HashMap<Object, Object>();
        }
        return arrays;
    }

    /**
     * 获取嵌套数据类型数组
     * 
     * @return 包含键类型和值类型的数组
     */
    public IDataType[] getNestedTypes() {
        return nestedTypes;
    }
}
