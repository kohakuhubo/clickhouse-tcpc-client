package com.berry.clickhouse.tcp.client.meta;

import com.berry.clickhouse.tcp.client.jdbc.ClickHouseTableMetaData;

import java.util.concurrent.ConcurrentHashMap;

/**
 * ClickHouseTableMetaDataManager类用于管理ClickHouse表的元数据
 * 提供注册和获取表元数据的功能
 */
public class ClickHouseTableMetaDataManager {

    private static final ConcurrentHashMap<String, ClickHouseTableMetaData> tables = new ConcurrentHashMap<>(); // 存储表元数据的并发哈希映射

    /**
     * 注册表元数据
     * 
     * @param tableMetaData ClickHouseTableMetaData实例
     */
    public void register(ClickHouseTableMetaData tableMetaData) {
        tables.putIfAbsent(tableMetaData.getTable(), tableMetaData); // 如果表名不存在，则注册表元数据
    }

    /**
     * 获取指定表的元数据
     * 
     * @param tableName 表名
     * @return ClickHouseTableMetaData实例，如果表不存在则返回null
     */
    public ClickHouseTableMetaData getTableMetaData(String tableName) {
        return tables.get(tableName); // 返回指定表的元数据
    }
}
