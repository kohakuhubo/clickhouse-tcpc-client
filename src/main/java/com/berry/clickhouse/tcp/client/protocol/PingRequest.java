/**
 * ClickHouse Ping请求类
 * 用于检测与ClickHouse服务器的连接是否有效
 * 发送此请求后，服务器将回复PongResponse
 */
package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;

/**
 * Ping请求实现类
 * 简单的请求类型，用于测试连接是否存活
 * 不包含任何额外数据
 */
public class PingRequest implements Request {

    /**
     * 单例实例，所有Ping请求共享此实例
     */
    public static final PingRequest INSTANCE = new PingRequest();

    /**
     * 获取请求类型
     * 
     * @return 请求类型（REQUEST_PING）
     */
    @Override
    public ProtoType type() {
        return ProtoType.REQUEST_PING;
    }

    /**
     * 写入请求内容
     * Ping请求不包含额外数据，因此此方法不执行任何操作
     * 
     * @param serializer 二进制序列化器
     * @throws IOException 如果写入操作失败
     */
    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException {
        // 不包含任何数据
    }
}
