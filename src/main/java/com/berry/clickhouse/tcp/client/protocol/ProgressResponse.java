/**
 * ClickHouse进度响应类
 * 表示查询执行的进度信息
 * 服务器定期发送此响应以告知客户端查询执行的进度
 */
package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.IOException;

/**
 * 进度响应实现类
 * 包含查询处理的统计信息
 * 如已处理的行数、字节数和总行数
 */
public class ProgressResponse implements Response {

    /**
     * 从二进制流中读取ProgressResponse对象
     * 
     * @param deserializer 二进制反序列化器
     * @return 新创建的ProgressResponse对象
     * @throws IOException 如果读取操作失败
     */
    public static ProgressResponse readFrom(BinaryDeserializer deserializer) throws IOException {
        return new ProgressResponse(
                deserializer.readVarInt(),
                deserializer.readVarInt(),
                deserializer.readVarInt()
        );
    }

    /**
     * 新处理的行数
     */
    private final long newRows;
    
    /**
     * 新处理的字节数
     */
    private final long newBytes;
    
    /**
     * 总处理行数
     */
    private final long newTotalRows;

    /**
     * 创建一个新的ProgressResponse实例
     * 
     * @param newRows 新处理的行数
     * @param newBytes 新处理的字节数
     * @param newTotalRows 总处理行数
     */
    public ProgressResponse(long newRows, long newBytes, long newTotalRows) {
        this.newRows = newRows;
        this.newBytes = newBytes;
        this.newTotalRows = newTotalRows;
    }

    /**
     * 获取响应类型
     * 
     * @return 响应类型（RESPONSE_PROGRESS）
     */
    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_PROGRESS;
    }

    /**
     * 获取新处理的行数
     * 
     * @return 新处理的行数
     */
    public long newRows() {
        return newRows;
    }

    /**
     * 获取新处理的字节数
     * 
     * @return 新处理的字节数
     */
    public long newBytes() {
        return newBytes;
    }

    /**
     * 获取总处理行数
     * 
     * @return 总处理行数
     */
    public long newTotalRows() {
        return newTotalRows;
    }

    /**
     * 返回进度响应的字符串表示
     * 
     * @return 包含进度信息的字符串
     */
    @Override
    public String toString() {
        return "ProgressResponse {" +
                "newRows=" + newRows +
                ", newBytes=" + newBytes +
                ", newTotalRows=" + newTotalRows +
                '}';
    }
}
