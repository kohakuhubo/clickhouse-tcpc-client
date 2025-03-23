package com.berry.clickhouse.tcp.client.log;

import java.util.ResourceBundle;

/**
 * JdkLogger类实现Logger接口
 * 使用Java内置的日志记录功能
 */
public class JdkLogger implements Logger {

    private static final Object[] EMPTY_ARRAY = new Object[]{}; // 空对象数组

    private final java.util.logging.Logger logger; // Java日志记录器

    /**
     * 构造函数，初始化JdkLogger实例
     * 
     * @param logger Java日志记录器
     */
    public JdkLogger(java.util.logging.Logger logger) {
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
        return this.logger.isLoggable(java.util.logging.Level.FINEST);
    }

    /**
     * 记录TRACE级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    @Override
    public void trace(String format, Object... arguments) {
        if (logger.isLoggable(java.util.logging.Level.FINEST)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, arguments);
            doLog(java.util.logging.Level.FINEST, ft);
        }
    }

    /**
     * 记录TRACE级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    @Override
    public void trace(String msg, Throwable t) {
        if (logger.isLoggable(java.util.logging.Level.FINEST)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(msg, EMPTY_ARRAY, t);
            doLog(java.util.logging.Level.FINEST, ft);
        }
    }

    /**
     * 检查是否启用DEBUG级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    @Override
    public boolean isDebugEnabled() {
        return this.logger.isLoggable(java.util.logging.Level.FINE);
    }

    /**
     * 记录DEBUG级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    @Override
    public void debug(String format, Object... arguments) {
        if (logger.isLoggable(java.util.logging.Level.FINE)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, arguments);
            doLog(java.util.logging.Level.FINE, ft);
        }
    }

    /**
     * 记录DEBUG级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    @Override
    public void debug(String msg, Throwable t) {
        if (logger.isLoggable(java.util.logging.Level.FINE)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(msg, EMPTY_ARRAY, t);
            doLog(java.util.logging.Level.FINE, ft);
        }
    }

    /**
     * 检查是否启用INFO级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    @Override
    public boolean isInfoEnabled() {
        return this.logger.isLoggable(java.util.logging.Level.INFO);
    }

    /**
     * 记录INFO级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    @Override
    public void info(String format, Object... arguments) {
        if (logger.isLoggable(java.util.logging.Level.INFO)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, arguments);
            doLog(java.util.logging.Level.INFO, ft);
        }
    }

    /**
     * 记录INFO级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    @Override
    public void info(String msg, Throwable t) {
        if (logger.isLoggable(java.util.logging.Level.INFO)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(msg, EMPTY_ARRAY, t);
            doLog(java.util.logging.Level.INFO, ft);
        }
    }

    /**
     * 检查是否启用WARN级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    @Override
    public boolean isWarnEnabled() {
        return this.logger.isLoggable(java.util.logging.Level.WARNING);
    }

    /**
     * 记录WARN级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    @Override
    public void warn(String format, Object... arguments) {
        if (logger.isLoggable(java.util.logging.Level.WARNING)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, arguments);
            doLog(java.util.logging.Level.WARNING, ft);
        }
    }

    /**
     * 记录WARN级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    @Override
    public void warn(String msg, Throwable t) {
        if (logger.isLoggable(java.util.logging.Level.WARNING)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(msg, EMPTY_ARRAY, t);
            doLog(java.util.logging.Level.WARNING, ft);
        }
    }

    /**
     * 检查是否启用ERROR级别的日志记录
     * 
     * @return true如果启用，false否则
     */
    @Override
    public boolean isErrorEnabled() {
        return this.logger.isLoggable(java.util.logging.Level.SEVERE);
    }

    /**
     * 记录ERROR级别的日志
     * 
     * @param format 日志格式
     * @param arguments 日志参数
     */
    @Override
    public void error(String format, Object... arguments) {
        if (logger.isLoggable(java.util.logging.Level.SEVERE)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, arguments);
            doLog(java.util.logging.Level.SEVERE, ft);
        }
    }

    /**
     * 记录ERROR级别的日志
     * 
     * @param msg 日志消息
     * @param t 异常信息
     */
    @Override
    public void error(String msg, Throwable t) {
        if (logger.isLoggable(java.util.logging.Level.SEVERE)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(msg, EMPTY_ARRAY, t);
            doLog(java.util.logging.Level.SEVERE, ft);
        }
    }

    /**
     * 执行日志记录
     * 
     * @param level 日志级别
     * @param ft 格式化的日志元组
     */
    private void doLog(java.util.logging.Level level, FormattingTuple ft) {
        logger.logrb(level, null, null, (ResourceBundle) null, ft.getMessage(), ft.getThrowable());
    }
}
