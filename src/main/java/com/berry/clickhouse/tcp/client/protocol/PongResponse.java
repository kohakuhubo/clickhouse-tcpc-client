/**
 * ClickHouse Pong响应类
 * 表示服务器对Ping请求的响应
 * 用于确认连接是否正常工作
 */
package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Pong响应实现类
 * 简单的响应类型，用于确认服务器存活
 * 不包含任何额外数据
 */
public class PongResponse implements Response {

    /**
     * 单例实例，所有Pong响应共享此实例
     */
    public static final PongResponse INSTANCE = new PongResponse();

    /**
     * 从二进制流中读取PongResponse对象
     * Pong响应不包含额外数据，因此仅返回单例实例
     * 
     * @param deserializer 二进制反序列化器
     * @return PongResponse单例实例
     * @throws IOException 如果读取操作失败
     * @throws SQLException 如果处理响应时发生SQL错误
     */
    public static PongResponse readFrom(BinaryDeserializer deserializer) throws IOException, SQLException {
        return INSTANCE;
    }

    /**
     * 获取响应类型
     * 
     * @return 响应类型（RESPONSE_PONG）
     */
    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_PONG;
    }
}
