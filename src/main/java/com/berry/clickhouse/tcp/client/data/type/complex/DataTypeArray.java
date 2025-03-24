/**
 * ClickHouse Array数据类型的实现
 * 用于处理ClickHouse中的数组类型，例如Array(Int32)、Array(String)等
 * 提供数组数据的序列化和反序列化功能
 */
package com.berry.clickhouse.tcp.client.data.type.complex;

import com.berry.clickhouse.tcp.client.data.DataTypeFactory;
import com.berry.clickhouse.tcp.client.data.IDataType;
import com.berry.clickhouse.tcp.client.data.type.DataTypeInt64;
import com.berry.clickhouse.tcp.client.jdbc.ClickHouseArray;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 数组数据类型实现
 * 处理ClickHouse中的Array类型，可以包含任意类型的元素
 */
public class DataTypeArray implements IDataType<Object> {

    /**
     * 数组类型创建器
     * 用于根据词法分析结果创建DataTypeArray实例
     */
    public static DataTypeCreator<Object> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        IDataType<?> arrayNestedType = DataTypeFactory.get(lexer, serverContext);
        Validate.isTrue(lexer.character() == ')');
        return new DataTypeArray("Array(" + arrayNestedType.name() + ")",
                arrayNestedType, (DataTypeInt64) DataTypeFactory.get("Int64", serverContext));
    };

    /**
     * 数据类型名称
     */
    private final String name;
    
    /**
     * 默认值
     */
    private final ClickHouseArray defaultValue;
    
    /**
     * 数组元素的数据类型
     */
    private final IDataType<?> elemDataType;
    
    /**
     * 用于存储偏移量的Int64数据类型
     */
    private final DataTypeInt64 offsetIDataType;

    /**
     * 创建数组数据类型
     * 
     * @param name 类型名称，如"Array(Int32)"
     * @param elemDataType 数组元素的数据类型
     * @param offsetIDataType 用于存储偏移量的Int64数据类型
     * @throws SQLException 如果创建过程中发生错误
     */
    public DataTypeArray(String name, IDataType<?> elemDataType, DataTypeInt64 offsetIDataType) throws SQLException {
        this.name = name;
        this.elemDataType = elemDataType;
        this.offsetIDataType = offsetIDataType;
        this.defaultValue = new ClickHouseArray(elemDataType, new Object[]{elemDataType.defaultValue()});
    }

    /**
     * 获取数据类型名称
     * 
     * @return 数据类型名称，如"Array(Int32)"
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * 获取数据类型默认值
     * 
     * @return 包含元素类型默认值的数组
     */
    @Override
    public ClickHouseArray defaultValue() {
        return defaultValue;
    }

    /**
     * 获取数据类型对应的Java类
     * 
     * @return ClickHouseArray.class
     */
    @Override
    public Class<Object> javaType() {
        return Object.class;
    }

    /**
     * 批量将数组序列化为二进制格式
     *
     * @param data 要序列化的数组数组
     * @param serializer 二进制序列化器
     * @throws SQLException 如果序列化过程中发生SQL错误
     * @throws IOException 如果序列化过程中发生I/O错误
     */
    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        Object[] value;
        if (data.getClass().isArray()) {
            value = (Object[]) data;
        } else if (data instanceof List) {
            value = ((List<?>) data).toArray();
        } else {
            value = ((ClickHouseArray) data).getArray();
        }
        for (Object f : value) {
            getElemDataType().serializeBinary(f, serializer);
        }
    }

    /**
     * 批量将数组序列化为二进制格式
     * 
     * @param data 要序列化的数组数组
     * @param serializer 二进制序列化器
     * @throws SQLException 如果序列化过程中发生SQL错误
     * @throws IOException 如果序列化过程中发生I/O错误
     */
    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        offsetIDataType.serializeBinary((long) data.length, serializer);
        getElemDataType().serializeBinaryBulk(data, serializer);
    }

    /**
     * 从二进制流反序列化数组
     * 
     * @param deserializer 二进制反序列化器
     * @return 反序列化后的数组
     * @throws SQLException 如果反序列化过程中发生SQL错误
     * @throws IOException 如果反序列化过程中发生I/O错误
     */
    @Override
    public ClickHouseArray deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Long offset = offsetIDataType.deserializeBinary(deserializer);
        Object[] data = getElemDataType().deserializeBinaryBulk(offset.intValue(), deserializer);
        return new ClickHouseArray(elemDataType, data);
    }

    /**
     * 批量从二进制流反序列化数组
     * 
     * @param rows 行数
     * @param deserializer 二进制反序列化器
     * @return 反序列化后的数组数组
     * @throws IOException 如果反序列化过程中发生I/O错误
     * @throws SQLException 如果反序列化过程中发生SQL错误
     */
    @Override
    public ClickHouseArray[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        ClickHouseArray[] arrays = new ClickHouseArray[rows];
        if (rows == 0) {
            return arrays;
        }

        // 读取所有行的偏移量
        int[] offsets = Arrays.stream(offsetIDataType.deserializeBinaryBulk(rows, deserializer)).mapToInt(value -> ((Long) value).intValue()).toArray();
        // 读取所有元素
        ClickHouseArray res = new ClickHouseArray(elemDataType,
                elemDataType.deserializeBinaryBulk(offsets[rows - 1], deserializer));

        // 根据偏移量切分为每行的数组
        for (int row = 0, lastOffset = 0; row < rows; row++) {
            int offset = offsets[row];
            arrays[row] = res.slice(lastOffset, offset - lastOffset);
            lastOffset = offset;
        }
        return arrays;
    }

    /**
     * 获取数组元素的数据类型
     * 
     * @return 元素数据类型
     */
    public IDataType getElemDataType() {
        return elemDataType;
    }
}
