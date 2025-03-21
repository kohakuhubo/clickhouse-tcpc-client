/**
 * ClickHouse会话状态枚举
 * 用于跟踪客户端与服务器会话的当前状态
 */
package com.berry.clickhouse.tcp.client;

/**
 * 会话状态枚举类
 * 定义了ClickHouse客户端与服务器会话的可能状态
 */
public enum SessionState {

    /**
     * 空闲状态
     * 表示会话当前没有活动的操作
     */
    IDLE, 
    
    /**
     * 等待插入状态
     * 表示会话正在等待数据插入操作完成
     */
    WAITING_INSERT
}
