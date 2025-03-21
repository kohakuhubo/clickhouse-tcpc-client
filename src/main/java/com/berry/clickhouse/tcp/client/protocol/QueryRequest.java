/**
 * ClickHouse查询请求类
 * 用于向ClickHouse服务器发送SQL查询请求
 */
package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;
import com.berry.clickhouse.tcp.client.serde.SettingType;
import com.berry.clickhouse.tcp.client.settings.SettingKey;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 查询请求实现类
 * 用于封装和发送SQL查询及相关设置
 */
public class QueryRequest implements Request {

    // Only read/have been read the columns specified in the query.
    /**
     * 只读取查询中指定的列的查询阶段
     */
    public static final int STAGE_FETCH_COLUMNS = 0;
    // Until the stage where the results of processing on different servers can be combined.
    /**
     * 执行到可以合并不同服务器处理结果的阶段
     */
    public static final int STAGE_WITH_MERGEABLE_STATE = 1;
    // Completely.
    /**
     * 完全执行查询的阶段
     */
    public static final int STAGE_COMPLETE = 2;
    // Until the stage where the aggregate functions were calculated and finalized.
    // It is used for auto distributed_group_by_no_merge optimization for distributed engine.
    // (See comments in StorageDistributed).
    /**
     * 执行到聚合函数计算并完成的阶段
     * 用于distributed_group_by_no_merge分布式引擎优化
     */
    public static final int STAGE_WITH_MERGEABLE_STATE_AFTER_AGGREGATION = 3;

    /**
     * 查询执行阶段
     */
    private final int stage;
    
    /**
     * 查询ID
     */
    private final String queryId;
    
    /**
     * SQL查询字符串
     */
    private final String queryString;
    
    /**
     * 是否启用压缩
     */
    private final boolean compression;
    
    /**
     * 客户端上下文
     */
    private final NativeContext.ClientContext clientContext;
    
    /**
     * 查询设置
     */
    private final Map<SettingKey, Serializable> settings;

    /**
     * 创建一个新的QueryRequest实例，使用默认设置
     * 
     * @param queryId 查询ID
     * @param clientContext 客户端上下文
     * @param stage 查询执行阶段
     * @param compression 是否启用压缩
     * @param queryString SQL查询字符串
     */
    public QueryRequest(String queryId, NativeContext.ClientContext clientContext, int stage, boolean compression, String queryString) {
        this(queryId, clientContext, stage, compression, queryString, new HashMap<>());
    }

    /**
     * 创建一个新的QueryRequest实例，指定查询设置
     * 
     * @param queryId 查询ID
     * @param clientContext 客户端上下文
     * @param stage 查询执行阶段
     * @param compression 是否启用压缩
     * @param queryString SQL查询字符串
     * @param settings 查询设置
     */
    public QueryRequest(String queryId, NativeContext.ClientContext clientContext, int stage, boolean compression, String queryString,
                        Map<SettingKey, Serializable> settings) {

        this.stage = stage;
        this.queryId = queryId;
        this.settings = settings;
        this.clientContext = clientContext;
        this.compression = compression;
        this.queryString = queryString;
    }

    /**
     * 获取请求类型
     * 
     * @return 请求类型（REQUEST_QUERY）
     */
    @Override
    public ProtoType type() {
        return ProtoType.REQUEST_QUERY;
    }

    /**
     * 将查询请求写入二进制序列化器
     * 
     * @param serializer 二进制序列化器
     * @throws IOException 如果写入操作失败
     * @throws SQLException 如果序列化过程中发生SQL错误
     */
    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException, SQLException {
        // 写入查询ID
        serializer.writeUTF8StringBinary(queryId);
        // 写入客户端上下文信息
        clientContext.writeTo(serializer);

        // 写入所有查询设置
        for (Map.Entry<SettingKey, Serializable> entry : settings.entrySet()) {
            serializer.writeUTF8StringBinary(entry.getKey().name());
            @SuppressWarnings("rawtypes")
            SettingType type = entry.getKey().type();
            //noinspection unchecked
            type.serializeSetting(serializer, entry.getValue());
        }
        // 写入空字符串表示设置结束
        serializer.writeUTF8StringBinary("");
        // 写入查询执行阶段
        serializer.writeVarInt(stage);
        // 写入是否启用压缩标志
        serializer.writeBoolean(compression);
        // 写入SQL查询字符串
        serializer.writeUTF8StringBinary(queryString);
        // 写入空数据块（向服务器发送空数据）
        DataRequest.EMPTY.writeTo(serializer);
    }
}
