/**
 * ClickHouse流结束响应类
 * 表示数据流传输结束，没有更多数据
 * 服务器在完成所有数据传输后发送此响应
 */
package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

/**
 * 流结束响应实现类
 * 标识查询或操作的数据流已经结束
 * 服务器通过此响应告知客户端不再发送更多数据
 */
public class EOFStreamResponse implements Response {

    /**
     * 单例实例，所有流结束响应共享此实例
     */
    public static final EOFStreamResponse INSTANCE = new EOFStreamResponse();

    /**
     * 从二进制流中读取EOFStreamResponse对象
     * 流结束响应不包含额外数据，因此仅返回单例实例
     * 
     * @param deserializer 二进制反序列化器
     * @return EOFStreamResponse单例实例
     */
    public static Response readFrom(BinaryDeserializer deserializer) {
        return INSTANCE;
    }

    /**
     * 获取响应类型
     * 
     * @return 响应类型（RESPONSE_END_OF_STREAM）
     */
    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_END_OF_STREAM;
    }
}
