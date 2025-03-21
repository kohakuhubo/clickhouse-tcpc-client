/**
 * ClickHouse数据响应类
 * 表示服务器返回的数据块响应，包含查询结果数据
 */
package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.NativeContext;
import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * 数据响应实现类
 * 包含服务器返回的数据块（Block）
 * 用于传输查询结果或表结构信息
 */
public class DataResponse implements Response {

    /**
     * 从二进制流中读取DataResponse对象
     * 
     * @param deserializer 二进制反序列化器
     * @param info 服务器上下文信息
     * @param serialize 是否序列化数据
     * @param block 可复用的数据块
     * @return 新创建的DataResponse对象
     * @throws IOException 如果读取操作失败
     * @throws SQLException 如果处理响应时发生SQL错误
     */
    public static DataResponse readFrom(
            BinaryDeserializer deserializer, NativeContext.ServerContext info, boolean serialize, Block block) throws IOException, SQLException {

        // 读取数据块名称
        String name = deserializer.readUTF8StringBinary();

        // 启用压缩（如果需要）
        deserializer.maybeEnableCompressed();
        Block newBlock;
        // 如果不需要序列化且提供了可复用的block，则使用复用模式
        if (!serialize && null != block) {
            newBlock = Block.readFrom(deserializer, block);
        } else {
            // 否则创建新的Block
            newBlock = Block.readFrom(deserializer, info, serialize);
        }
        // 禁用压缩
        deserializer.maybeDisableCompressed();
        return new DataResponse(name, newBlock);
    }

    /**
     * 数据块名称
     */
    private final String name;

    /**
     * 数据块内容
     */
    private final Block block;

    /**
     * 创建一个新的DataResponse实例
     * 
     * @param name 数据块名称
     * @param block 数据块内容
     */
    public DataResponse(String name, Block block) {
        this.name = name;
        this.block = block;
    }

    /**
     * 获取响应类型
     * 
     * @return 响应类型（RESPONSE_DATA）
     */
    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_DATA;
    }

    /**
     * 获取数据块名称
     * 
     * @return 数据块名称
     */
    public String name() {
        return name;
    }

    /**
     * 获取数据块内容
     * 
     * @return 数据块对象
     */
    public Block block() {
        return block;
    }
}
