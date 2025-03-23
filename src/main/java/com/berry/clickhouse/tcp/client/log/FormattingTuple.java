package com.berry.clickhouse.tcp.client.log;

/**
 * FormattingTuple类用于存储格式化后的日志消息和参数
 * 包含日志消息、参数数组和异常信息
 */
public class FormattingTuple {

    private final String message; // 格式化后的日志消息
    private final Throwable throwable; // 异常信息
    private final Object[] argArray; // 日志参数数组

    /**
     * 构造函数，初始化FormattingTuple实例
     * 
     * @param message 格式化后的日志消息
     */
    public FormattingTuple(String message) {
        this(message, null, null);
    }

    /**
     * 构造函数，初始化FormattingTuple实例
     * 
     * @param message 格式化后的日志消息
     * @param argArray 日志参数数组
     * @param throwable 异常信息
     */
    public FormattingTuple(String message, Object[] argArray, Throwable throwable) {
        this.message = message;
        this.throwable = throwable;
        this.argArray = argArray;
    }

    /**
     * 获取格式化后的日志消息
     * 
     * @return 格式化后的日志消息
     */
    public String getMessage() {
        return message;
    }

    /**
     * 获取日志参数数组
     * 
     * @return 日志参数数组
     */
    public Object[] getArgArray() {
        return argArray;
    }

    /**
     * 获取异常信息
     * 
     * @return 异常信息
     */
    public Throwable getThrowable() {
        return throwable;
    }
}
