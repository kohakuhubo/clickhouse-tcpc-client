package com.berry.clickhouse.tcp.client.jdbc;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.util.BinarySerializerUtil;

import java.util.List;
import java.util.Set;

public class ClickHouseTableMetaData {

    private final String table;

    private final List<String> columnNames;

    private final List<String> columnTypes;

    private final byte[][] colNameBytes;

    private NativeContext.ServerContext serverContext;

    private final Set<String> systemBufferColumns;

    public ClickHouseTableMetaData(String table, List<String> columnNames, List<String> columnTypes, Set<String> systemBufferColumns) {
        this.table = table;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.systemBufferColumns = systemBufferColumns;
        this.colNameBytes = new byte[columnNames.size()][];
        for (int i = 0; i < columnNames.size(); i++) {
            colNameBytes[i] = BinarySerializerUtil.serializeString(columnNames.get(i));
        }
    }

    public String getTable() {
        return table;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public byte[][] getColNameBytes() {
        return colNameBytes;
    }

    public NativeContext.ServerContext getServerContext() {
        return serverContext;
    }

    public void setServerContext(NativeContext.ServerContext serverContext) {
        this.serverContext = serverContext;
    }

    public Set<String> getSystemBufferColumns() {
        return systemBufferColumns;
    }
}
