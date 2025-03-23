package com.berry.clickhouse.tcp.client.stream;

import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.misc.CheckedIterator;
import com.berry.clickhouse.tcp.client.protocol.DataResponse;

import java.sql.SQLException;

/**
 * QueryResult接口定义了查询结果的基本操作
 * 包括获取结果头部和数据迭代器
 */
public interface QueryResult {

    /**
     * 获取查询结果的头部
     * 
     * @return 查询结果头部
     * @throws SQLException SQL异常
     */
    Block header() throws SQLException;

    /**
     * 获取数据迭代器
     * 
     * @return 数据迭代器
     */
    CheckedIterator<DataResponse, SQLException> data();
}
