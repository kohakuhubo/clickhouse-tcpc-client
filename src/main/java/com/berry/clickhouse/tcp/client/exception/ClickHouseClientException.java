/**
 * ClickHouse客户端异常类
 * 表示在ClickHouse客户端操作过程中发生的异常
 * 使用预定义的CLIENT_ERROR错误代码
 */
package com.berry.clickhouse.tcp.client.exception;

import com.berry.clickhouse.tcp.client.settings.ClickHouseErrCode;

/**
 * ClickHouse客户端异常
 * 用于客户端操作过程中发生的非服务器端错误
 */
public class ClickHouseClientException extends ClickHouseException {

    /**
     * 创建ClickHouseClientException实例
     * 
     * @param message 错误消息
     */
    public ClickHouseClientException(String message) {
        super(ClickHouseErrCode.CLIENT_ERROR.code(), message);
    }

    /**
     * 创建ClickHouseClientException实例
     * 
     * @param message 错误消息
     * @param cause 原始异常
     */
    public ClickHouseClientException(String message, Throwable cause) {
        super(ClickHouseErrCode.CLIENT_ERROR.code(), message, cause);
    }

    /**
     * 创建ClickHouseClientException实例
     * 
     * @param cause 原始异常
     */
    public ClickHouseClientException(Throwable cause) {
        super(ClickHouseErrCode.CLIENT_ERROR.code(), cause);
    }
}
