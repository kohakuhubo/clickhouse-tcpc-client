package com.berry.clickhouse.tcp.client.jdbc;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.util.BinarySerializerUtil;

import java.util.List;
import java.util.Set;

/**
 * ClickHouseTableMetaData类表示ClickHouse表的元数据
 * 包含表名、列名、列类型等信息
 */
public class ClickHouseTableMetaData {

    private final String table; // 表名
    private final List<String> columnNames; // 列名列表
    private final List<String> columnTypes; // 列类型列表
    private final byte[][] colNameBytes; // 列名的字节数组
    private NativeContext.ServerContext serverContext; // 服务器上下文
    private final Set<String> systemBufferColumns; // 系统缓冲列

    /**
     * 构造函数，初始化ClickHouseTableMetaData实例
     * 
     * @param table 表名
     * @param columnNames 列名列表
     * @param columnTypes 列类型列表
     * @param systemBufferColumns 系统缓冲列集合
     */
    public ClickHouseTableMetaData(String table, List<String> columnNames, List<String> columnTypes, Set<String> systemBufferColumns) {
        this.table = table;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.systemBufferColumns = systemBufferColumns;
        this.colNameBytes = new byte[columnNames.size()][];
        for (int i = 0; i < columnNames.size(); i++) {
            colNameBytes[i] = BinarySerializerUtil.serializeString(columnNames.get(i)); // 序列化列名
        }
    }

    /**
     * 获取表名
     * 
     * @return 表名
     */
    public String getTable() {
        return table; // 返回表名
    }

    /**
     * 获取列名列表
     * 
     * @return 列名列表
     */
    public List<String> getColumnNames() {
        return columnNames; // 返回列名列表
    }

    /**
     * 获取列类型列表
     * 
     * @return 列类型列表
     */
    public List<String> getColumnTypes() {
        return columnTypes; // 返回列类型列表
    }

    /**
     * 获取列名的字节数组
     * 
     * @return 列名的字节数组
     */
    public byte[][] getColNameBytes() {
        return colNameBytes; // 返回列名的字节数组
    }

    /**
     * 获取服务器上下文
     * 
     * @return 服务器上下文
     */
    public NativeContext.ServerContext getServerContext() {
        return serverContext; // 返回服务器上下文
    }

    /**
     * 设置服务器上下文
     * 
     * @param serverContext 服务器上下文
     */
    public void setServerContext(NativeContext.ServerContext serverContext) {
        this.serverContext = serverContext; // 设置服务器上下文
    }

    /**
     * 获取系统缓冲列集合
     * 
     * @return 系统缓冲列集合
     */
    public Set<String> getSystemBufferColumns() {
        return systemBufferColumns; // 返回系统缓冲列集合
    }
}
