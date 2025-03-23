/**
 * ClickHouse查询进度监听器的默认实现
 * 提供一个简单的进度监听实现，将进度信息记录到日志
 */
package com.berry.clickhouse.tcp.client.protocol.listener;

import com.berry.clickhouse.tcp.client.log.Logger;
import com.berry.clickhouse.tcp.client.log.LoggerFactory;
import com.berry.clickhouse.tcp.client.protocol.ProgressResponse;

/**
 * 默认查询进度监听器
 * 实现ProgressListener接口，将进度信息记录到日志
 * 作为简单的进度监控使用
 */
public class DefaultProgressListener implements ProgressListener {

    /**
     * 日志记录器
     */
    private static final Logger LOG = LoggerFactory.getLogger(DefaultProgressListener.class);

    /**
     * 私有构造函数
     * 防止外部直接实例化，应使用create()工厂方法创建实例
     */
    private DefaultProgressListener() {
    }

    /**
     * 创建DefaultProgressListener实例的工厂方法
     * 
     * @return 新创建的DefaultProgressListener实例
     */
    public static DefaultProgressListener create() {
        return new DefaultProgressListener();
    }

    /**
     * 处理进度更新
     * 将进度信息记录到日志
     * 
     * @param progressResponse 包含进度信息的响应对象
     */
    @Override
    public void onProgress(ProgressResponse progressResponse) {
        LOG.info("DefaultProgressListener: ".concat(progressResponse.toString()));
    }
}
