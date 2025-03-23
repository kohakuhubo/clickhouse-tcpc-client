package com.berry.clickhouse.tcp.client.stream;

import com.berry.clickhouse.tcp.client.data.Block;

import java.sql.SQLException;

/**
 * NativeInputFormat接口扩展了InputFormat接口
 * 定义了原生输入格式的填充方法
 */
public interface NativeInputFormat extends InputFormat<Block, SQLException> {

    @Override
    default String name() {
        return "Native"; // 返回原生输入格式名称
    }

    /**
     * 填充数据到指定的块中
     * 
     * @param block 数据块
     * @throws SQLException SQL异常
     */
    void fill(Block block) throws SQLException;
}
