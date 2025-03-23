package com.berry.clickhouse.tcp.client.stream;

import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.misc.CheckedIterator;
import com.berry.clickhouse.tcp.client.misc.CheckedSupplier;
import com.berry.clickhouse.tcp.client.protocol.DataResponse;
import com.berry.clickhouse.tcp.client.protocol.EOFStreamResponse;
import com.berry.clickhouse.tcp.client.protocol.ProgressResponse;
import com.berry.clickhouse.tcp.client.protocol.Response;
import com.berry.clickhouse.tcp.client.protocol.listener.ProgressListener;

import java.sql.SQLException;
import java.util.List;

/**
 * ClickHouseQueryResult类实现了QueryResult接口
 * 用于处理ClickHouse查询的结果，包括数据块和进度监听
 */
public class ClickHouseQueryResult implements QueryResult {
    private final CheckedSupplier<Response, SQLException> responseSupplier; // 响应供应商
    private ProgressListener progressListener; // 进度监听器
    private Block header; // 查询结果头部
    private Block currentBlock; // 当前数据块
    private List<Block> blocks; // 数据块列表
    private boolean atEnd; // 是否到达结果末尾
    private int nextIndexBlockIndex = 1; // 下一个数据块索引
    private CheckedIterator<DataResponse, SQLException> data; // 数据迭代器

    /**
     * 构造函数，使用数据块列表初始化ClickHouseQueryResult
     * 
     * @param blocks 数据块列表
     */
    public ClickHouseQueryResult(List<Block> blocks) {
        this.responseSupplier = null;
        this.blocks = blocks;
    }

    /**
     * 构造函数，使用响应供应商初始化ClickHouseQueryResult
     * 
     * @param responseSupplier 响应供应商
     */
    public ClickHouseQueryResult(CheckedSupplier<Response, SQLException> responseSupplier) {
        this.responseSupplier = responseSupplier;
        this.data = data();
    }

    /**
     * 设置进度监听器
     * 
     * @param progressListener 进度监听器
     */
    public void setProgressListener(ProgressListener progressListener) {
        this.progressListener = progressListener;
    }

    @Override
    public Block header() throws SQLException {
        ensureHeaderConsumed(); // 确保头部已被消费
        return header; // 返回查询结果头部
    }

    @Override
    public CheckedIterator<DataResponse, SQLException> data() {
        return new CheckedIterator<DataResponse, SQLException>() {

            private DataResponse current; // 当前数据响应

            @Override
            public boolean hasNext() throws SQLException {
                return current != null || fill() != null; // 检查是否有下一个元素
            }

            @Override
            public DataResponse next() throws SQLException {
                return drain(); // 获取下一个数据响应
            }

            private DataResponse fill() throws SQLException {
                ensureHeaderConsumed(); // 确保头部已被消费
                return current = consumeDataResponse(); // 消费数据响应
            }

            private DataResponse drain() throws SQLException {
                if (current == null) {
                    fill(); // 如果当前为空，则填充
                }

                DataResponse top = current; // 获取当前数据响应
                current = null; // 清空当前
                return top; // 返回数据响应
            }
        };
    }

    private void ensureHeaderConsumed() throws SQLException {
        if (header == null) {
            DataResponse firstDataResponse = consumeDataResponse(); // 消费第一个数据响应
            header = firstDataResponse != null ? firstDataResponse.block() : new Block(); // 设置头部
        }
    }

    private DataResponse consumeDataResponse() throws SQLException {
        long readRows = 0; // 读取的行数
        long readBytes = 0; // 读取的字节数
        while (!atEnd) {
            Response response = responseSupplier.get(); // 获取响应
            if (response instanceof DataResponse) {
                DataResponse dataResponse = (DataResponse) response; // 转换为数据响应
                dataResponse.block().setProgress(readRows, readBytes); // 设置进度
                return dataResponse; // 返回数据响应
            } else if (response instanceof EOFStreamResponse || response == null) {
                atEnd = true; // 到达末尾
            } else if (response instanceof ProgressResponse) {
                if (progressListener != null) {
                    progressListener.onProgress((ProgressResponse) response); // 通知进度监听器
                }
                readRows += ((ProgressResponse) response).newRows(); // 更新读取的行数
                readBytes += ((ProgressResponse) response).newBytes(); // 更新读取的字节数
            }
        }

        return null; // 返回空
    }
}
