/**
 * ClickHouse查询进度监听器接口
 * 用于接收和处理查询执行过程中的进度通知
 * 允许客户端代码响应查询执行的进度更新
 */
package com.berry.clickhouse.tcp.client.protocol.listener;

import com.berry.clickhouse.tcp.client.protocol.ProgressResponse;

/**
 * 查询进度监听器接口
 * 实现此接口可以接收查询执行期间的进度更新
 */
public interface ProgressListener {
    /**
     * 当收到进度更新时调用此方法
     * 
     * @param progressResponse 包含进度信息的响应对象
     */
    void onProgress(ProgressResponse progressResponse);
}
