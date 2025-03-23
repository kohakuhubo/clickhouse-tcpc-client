/**
 * ClickHouse Hello请求类
 * 用于与ClickHouse服务器建立连接并进行身份验证
 * 这是客户端发送的第一个请求，包含客户端信息和身份验证凭据
 */
package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.serde.BinarySerializer;
import com.berry.clickhouse.tcp.client.settings.ClickHouseDefines;

import java.io.IOException;

/**
 * Hello请求实现类
 * 用于建立与服务器的初始连接并进行身份验证
 * 发送客户端信息、版本和认证信息
 */
public class HelloRequest implements Request {

    /**
     * 客户端名称
     */
    private final String clientName;
    
    /**
     * 客户端修订版本号
     */
    private final long clientReversion;
    
    /**
     * 默认数据库名称
     */
    private final String defaultDatabase;
    
    /**
     * 客户端用户名
     */
    private final String clientUsername;
    
    /**
     * 客户端密码
     */
    private final String clientPassword;

    /**
     * 创建一个新的HelloRequest实例
     * 
     * @param clientName 客户端名称
     * @param clientReversion 客户端修订版本号
     * @param defaultDatabase 默认数据库名称
     * @param clientUsername 客户端用户名
     * @param clientPassword 客户端密码
     */
    public HelloRequest(String clientName, long clientReversion, String defaultDatabase,
                        String clientUsername, String clientPassword) {
        this.clientName = clientName;
        this.clientReversion = clientReversion;
        this.defaultDatabase = defaultDatabase;
        this.clientUsername = clientUsername;
        this.clientPassword = clientPassword;
    }

    /**
     * 获取请求类型
     * 
     * @return 请求类型（REQUEST_HELLO）
     */
    @Override
    public ProtoType type() {
        return ProtoType.REQUEST_HELLO;
    }

    /**
     * 将Hello请求写入二进制序列化器
     * 写入顺序：客户端名称、主版本号、次版本号、修订版本号、默认数据库、用户名、密码
     * 
     * @param serializer 二进制序列化器
     * @throws IOException 如果写入操作失败
     */
    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException {
        serializer.writeUTF8StringBinary(ClickHouseDefines.NAME + " " + clientName);
        serializer.writeVarInt(ClickHouseDefines.MAJOR_VERSION);
        serializer.writeVarInt(ClickHouseDefines.MINOR_VERSION);
        serializer.writeVarInt(clientReversion);
        serializer.writeUTF8StringBinary(defaultDatabase);
        serializer.writeUTF8StringBinary(clientUsername);
        serializer.writeUTF8StringBinary(clientPassword);
    }
}
