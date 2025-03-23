package com.berry.clickhouse.tcp.client.log;

/**
 * Logger接口定义了日志记录的基本操作
 * 提供不同级别的日志记录方法
 */
public interface Logger {

    /**
     * 获取日志记录器的名称
     * 
     * @return 日志记录器的名称
     */
    String getName();

    /**
     * 检查是否启用TRACE级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    boolean isTraceEnabled();

    /**
     * 记录TRACE级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    void trace(String format, Object... arguments);

    /**
     * 记录TRACE级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    void trace(String msg, Throwable t);

    /**
     * 检查是否启用DEBUG级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    boolean isDebugEnabled();

    /**
     * 记录DEBUG级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    void debug(String format, Object... arguments);

    /**
     * 记录DEBUG级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    void debug(String msg, Throwable t);

    /**
     * 检查是否启用INFO级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    boolean isInfoEnabled();

    /**
     * 记录INFO级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    void info(String format, Object... arguments);

    /**
     * 记录INFO级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    void info(String msg, Throwable t);

    /**
     * 检查是否启用WARN级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    boolean isWarnEnabled();

    /**
     * 记录WARN级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    void warn(String format, Object... arguments);

    /**
     * 记录WARN级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    void warn(String msg, Throwable t);

    /**
     * 检查是否启用ERROR级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    boolean isErrorEnabled();

    /**
     * 记录ERROR级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    void error(String format, Object... arguments);

    /**
     * 记录ERROR级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    void error(String msg, Throwable t);
}
