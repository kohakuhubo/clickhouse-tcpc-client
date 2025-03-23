/**
 * ClickHouse SQL异常类
 * 表示在执行SQL操作时从ClickHouse服务器返回的异常
 * 继承自标准JDBC SQLException
 */
package com.berry.clickhouse.tcp.client.exception;

import java.sql.SQLException;

/**
 * ClickHouse SQL异常
 * 用于表示ClickHouse服务器返回的SQL执行错误
 */
public class ClickHouseSQLException extends SQLException {

    /**
     * 创建ClickHouseSQLException实例
     * 
     * @param code 错误代码
     * @param message 错误消息
     */
    public ClickHouseSQLException(int code, String message) {
        this(code, message, null);
    }

    /**
     * 创建ClickHouseSQLException实例
     * 
     * @param code 错误代码
     * @param message 错误消息
     * @param cause 原始异常
     */
    public ClickHouseSQLException(int code, String message, Throwable cause) {
        super(message, null, code, cause);
    }
}
