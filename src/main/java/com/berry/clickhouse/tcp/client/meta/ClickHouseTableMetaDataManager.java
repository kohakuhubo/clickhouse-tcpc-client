package com.berry.clickhouse.tcp.client.meta;

import com.berry.clickhouse.tcp.client.jdbc.ClickHouseTableMetaData;

import java.util.concurrent.ConcurrentHashMap;

public class ClickHouseTableMetaDataManager {

    private static final ConcurrentHashMap<String, ClickHouseTableMetaData> tables = new ConcurrentHashMap<>();

    public void register(ClickHouseTableMetaData tableMetaData) {
        tables.putIfAbsent(tableMetaData.getTable(), tableMetaData);
    }

    public ClickHouseTableMetaData getTableMetaData(String tableName) {
        return tables.get(tableName);
    }

}
