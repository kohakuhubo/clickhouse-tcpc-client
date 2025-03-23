/**
 * ClickHouse客户端运行时异常基类
 * 所有ClickHouse客户端相关的自定义运行时异常都继承自此类
 * 提供错误代码和错误信息
 */
package com.berry.clickhouse.tcp.client.exception;

/**
 * ClickHouse客户端运行时异常基类
 * 用于表示与ClickHouse交互过程中发生的异常情况
 */
public class ClickHouseException extends RuntimeException {

    /**
     * 错误代码
     */
    protected int errCode;

    /**
     * 创建ClickHouseException实例
     * 
     * @param errCode 错误代码
     * @param message 错误消息
     */
    public ClickHouseException(int errCode, String message) {
        super(message);
        this.errCode = errCode;
    }

    /**
     * 创建ClickHouseException实例
     * 
     * @param errCode 错误代码
     * @param message 错误消息
     * @param cause 原始异常
     */
    public ClickHouseException(int errCode, String message, Throwable cause) {
        super(message, cause);
        this.errCode = errCode;
    }

    /**
     * 创建ClickHouseException实例
     * 
     * @param errCode 错误代码
     * @param cause 原始异常
     */
    public ClickHouseException(int errCode, Throwable cause) {
        super(cause);
        this.errCode = errCode;
    }

    /**
     * 获取错误代码
     * 
     * @return 错误代码
     */
    public int errCode() {
        return errCode;
    }
}
