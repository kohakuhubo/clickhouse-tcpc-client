/**
 * ClickHouse数据请求类
 * 用于向ClickHouse服务器发送数据块
 * 主要用于INSERT操作中传输数据
 */
package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * 数据请求实现类
 * 用于向服务器发送命名数据块
 * 包括空数据块（表示完成）和包含实际数据的数据块
 */
public class DataRequest implements Request {

    /**
     * 空数据请求，表示数据传输完成
     * 通常在发送完所有数据块后发送此请求
     */
    public static final DataRequest EMPTY = new DataRequest("");

    /**
     * 数据块名称
     */
    private final String name;
    
    /**
     * 要发送的数据块
     */
    private final Block block;

    /**
     * 创建一个空的数据请求
     * 
     * @param name 数据块名称
     */
    public DataRequest(String name) {
        this(name, new Block());
    }

    /**
     * 创建一个包含数据块的数据请求
     * 
     * @param name 数据块名称
     * @param block 要发送的数据块
     */
    public DataRequest(String name, Block block) {
        this.name = name;
        this.block = block;
    }

    /**
     * 获取请求类型
     * 
     * @return 请求类型（REQUEST_DATA）
     */
    @Override
    public ProtoType type() {
        return ProtoType.REQUEST_DATA;
    }

    /**
     * 将数据请求写入二进制序列化器
     * 先写入数据块名称，然后写入数据块
     * 支持数据压缩
     * 
     * @param serializer 二进制序列化器
     * @throws IOException 如果写入操作失败
     * @throws SQLException 如果序列化过程中发生SQL错误
     */
    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException, SQLException {
        serializer.writeUTF8StringBinary(name);

        serializer.maybeEnableCompressed();
        block.writeTo(serializer);
        serializer.maybeDisableCompressed();
    }
}
