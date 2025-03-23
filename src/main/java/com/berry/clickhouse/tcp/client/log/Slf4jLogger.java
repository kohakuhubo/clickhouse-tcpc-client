package com.berry.clickhouse.tcp.client.log;

/**
 * Slf4jLogger类实现Logger接口
 * 使用SLF4J日志记录功能
 */
public class Slf4jLogger implements Logger {

    private final org.slf4j.Logger logger; // SLF4J日志记录器

    /**
     * 构造函数，初始化Slf4jLogger实例
     * 
     * @param logger SLF4J日志记录器
     */
    public Slf4jLogger(org.slf4j.Logger logger) {
        this.logger = logger;
    }

    /**
     * 获取日志记录器的名称
     * 
     * @return 日志记录器的名称
     */
    @Override
    public String getName() {
        return this.logger.getName();
    }

    /**
     * 检查是否启用TRACE级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    @Override
    public boolean isTraceEnabled() {
        return this.logger.isTraceEnabled();
    }

    /**
     * 记录TRACE级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    @Override
    public void trace(String format, Object... arguments) {
        this.logger.trace(format, arguments);
    }

    /**
     * 记录TRACE级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    @Override
    public void trace(String msg, Throwable t) {
        this.logger.trace(msg, t);
    }

    /**
     * 检查是否启用DEBUG级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    @Override
    public boolean isDebugEnabled() {
        return this.logger.isDebugEnabled();
    }

    /**
     * 记录DEBUG级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    @Override
    public void debug(String format, Object... arguments) {
        this.logger.debug(format, arguments);
    }

    /**
     * 记录DEBUG级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    @Override
    public void debug(String msg, Throwable t) {
        this.logger.debug(msg, t);
    }

    /**
     * 检查是否启用INFO级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    @Override
    public boolean isInfoEnabled() {
        return this.logger.isInfoEnabled();
    }

    /**
     * 记录INFO级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    @Override
    public void info(String format, Object... arguments) {
        this.logger.info(format, arguments);
    }

    /**
     * 记录INFO级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    @Override
    public void info(String msg, Throwable t) {
        this.logger.info(msg, t);
    }

    /**
     * 检查是否启用WARN级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    @Override
    public boolean isWarnEnabled() {
        return this.logger.isWarnEnabled();
    }

    /**
     * 记录WARN级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    @Override
    public void warn(String format, Object... arguments) {
        this.logger.warn(format, arguments);
    }

    /**
     * 记录WARN级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    @Override
    public void warn(String msg, Throwable t) {
        this.logger.warn(msg, t);
    }

    /**
     * 检查是否启用ERROR级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    @Override
    public boolean isErrorEnabled() {
        return this.logger.isErrorEnabled();
    }

    /**
     * 记录ERROR级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    @Override
    public void error(String format, Object... arguments) {
        this.logger.error(format, arguments);
    }

    /**
     * 记录ERROR级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    @Override
    public void error(String msg, Throwable t) {
        this.logger.error(msg, t);
    }
}
