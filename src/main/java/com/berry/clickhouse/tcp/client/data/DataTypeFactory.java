package com.berry.clickhouse.tcp.client.data;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.data.type.*;
import com.berry.clickhouse.tcp.client.data.type.complex.*;
import com.berry.clickhouse.tcp.client.misc.LRUCache;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;
import com.berry.clickhouse.tcp.client.settings.ClickHouseDefines;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * DataTypeFactory类用于创建ClickHouse数据类型的实例
 * 提供数据类型的缓存和解析功能
 */
public class DataTypeFactory {
    private static final LRUCache<String, IDataType<?>> DATA_TYPE_CACHE = new LRUCache<>(ClickHouseDefines.DATA_TYPE_CACHE_SIZE);

    /**
     * 根据数据类型名称获取对应的IDataType实例
     * 
     * @param type 数据类型名称
     * @param serverContext 服务器上下文
     * @return 对应的IDataType实例
     * @throws SQLException 如果发生SQL错误
     */
    public static IDataType<?> get(String type, NativeContext.ServerContext serverContext) throws SQLException {
        IDataType<?> dataType = DATA_TYPE_CACHE.get(type);
        if (dataType != null) {
            DATA_TYPE_CACHE.put(type, dataType);
            return dataType;
        }

        SQLLexer lexer = new SQLLexer(0, type);
        dataType = get(lexer, serverContext);
        Validate.isTrue(lexer.eof());

        DATA_TYPE_CACHE.put(type, dataType);
        return dataType;
    }

    private static final Map<String, IDataType<?>> dataTypes = initialDataTypes();

    /**
     * 根据SQLLexer解析数据类型
     * 
     * @param lexer SQL词法分析器
     * @param serverContext 服务器上下文
     * @return 对应的IDataType实例
     * @throws SQLException 如果发生SQL错误
     */
    public static IDataType<?> get(SQLLexer lexer, NativeContext.ServerContext serverContext) throws SQLException {
        String dataTypeName = String.valueOf(lexer.bareWord());

        if (dataTypeName.equalsIgnoreCase("Tuple")) {
            return DataTypeTuple.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Array")) {
            return DataTypeArray.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Enum8")) {
            return DataTypeEnum8.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Enum16")) {
            return DataTypeEnum16.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("DateTime")) {
            return DataTypeDateTime.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("DateTime64")) {
            return DataTypeDateTime64.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Nullable")) {
            return DataTypeNullable.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("LowCardinality")) {
            return DataTypeLowCardinality.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("FixedString") || dataTypeName.equals("Binary")) {
            return DataTypeFixedString.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Decimal")) {
            return DataTypeDecimal.creator.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("String")) {
            return DataTypeString.CREATOR.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Nothing")) {
            return DataTypeNothing.CREATOR.createDataType(lexer, serverContext);
        } else if (dataTypeName.equalsIgnoreCase("Map")) {
            return DataTypeMap.creator.createDataType(lexer, serverContext);
        } else {
            IDataType<?> dataType = dataTypes.get(dataTypeName.toLowerCase(Locale.ROOT));
            Validate.isTrue(dataType != null, "Unknown data type: " + dataTypeName);
            return dataType;
        }
    }

    private static Map<String, IDataType<?>> initialDataTypes() {
        Map<String, IDataType<?>> creators = new HashMap<>();

        registerType(creators, new DataTypeIPv4());
        registerType(creators, new DataTypeIPv6());
        registerType(creators, new DataTypeUUID());
        registerType(creators, new DataTypeFloat32());
        registerType(creators, new DataTypeFloat64());

        registerType(creators, new DataTypeInt8());
        registerType(creators, new DataTypeInt16());
        registerType(creators, new DataTypeInt32());
        registerType(creators, new DataTypeInt64());

        registerType(creators, new DataTypeUInt8());
        registerType(creators, new DataTypeUInt16());
        registerType(creators, new DataTypeUInt32());
        registerType(creators, new DataTypeUInt64());

        registerType(creators, new DataTypeDate());
        registerType(creators, new DataTypeDate32());
        return creators;
    }

    private static void registerType(Map<String, IDataType<?>> creators, IDataType<?> type) {
        creators.put(type.name().toLowerCase(Locale.ROOT), type);
        for (String typeName : type.getAliases()) {
            creators.put(typeName.toLowerCase(Locale.ROOT), type);
        }
    }

    private static Map<String, DataTypeCreator<?>> initComplexDataTypes() {
        return new HashMap<>();
    }

    private static void registerComplexType(
            Map<String, DataTypeCreator<?>> creators, IDataType<?> type, DataTypeCreator<?> creator) {

        creators.put(type.name().toLowerCase(Locale.ROOT), creator);
        for (String typeName : type.getAliases()) {
            creators.put(typeName.toLowerCase(Locale.ROOT), creator);
        }
    }
}
