/**
 * ClickHouse TCP协议请求接口
 * 定义了所有向ClickHouse服务器发送的请求的基本结构
 */
package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * ClickHouse客户端请求接口
 * 所有发送给服务器的请求都实现此接口
 */
public interface Request {

    /**
     * 获取请求类型
     * 
     * @return 请求类型枚举值
     */
    ProtoType type();

    /**
     * 将请求内容写入序列化器
     * 
     * @param serializer 二进制序列化器
     * @throws IOException 如果写入操作失败
     * @throws SQLException 如果序列化过程中发生SQL错误
     */
    void writeImpl(BinarySerializer serializer) throws IOException, SQLException;

    /**
     * 将完整请求（包括类型标识）写入序列化器
     * 默认实现会先写入请求类型，然后调用writeImpl写入具体内容
     * 
     * @param serializer 二进制序列化器
     * @throws IOException 如果写入操作失败
     * @throws SQLException 如果序列化过程中发生SQL错误
     */
    default void writeTo(BinarySerializer serializer) throws IOException, SQLException {
        serializer.writeVarInt(type().id());
        this.writeImpl(serializer);
    }

    /**
     * ClickHouse TCP协议请求类型枚举
     * 定义了客户端可以发送的各种请求类型及其对应的ID
     */
    enum ProtoType {
        /**
         * Hello请求，用于建立连接并进行身份验证
         */
        REQUEST_HELLO(0),
        
        /**
         * 查询请求，用于发送SQL查询
         */
        REQUEST_QUERY(1),
        
        /**
         * 数据请求，用于发送数据块（如INSERT语句的数据）
         */
        REQUEST_DATA(2),
        
        /**
         * Ping请求，用于检测连接是否有效
         */
        REQUEST_PING(4);

        /**
         * 请求类型ID
         */
        private final int id;

        /**
         * 构造函数
         * 
         * @param id 请求类型ID
         */
        ProtoType(int id) {
            this.id = id;
        }

        /**
         * 获取请求类型ID
         * 
         * @return 请求类型ID
         */
        public long id() {
            return id;
        }
    }
}
