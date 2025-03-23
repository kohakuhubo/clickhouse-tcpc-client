package com.berry.clickhouse.tcp.client.log;

/**
 * LoggerFactory类用于创建Logger实例
 * 根据不同的日志框架适配器返回相应的Logger实现
 */
public class LoggerFactory {

    private static LoggerFactoryAdaptor adaptor; // 日志适配器

    static {
        try {
            if (org.slf4j.LoggerFactory.getILoggerFactory() != null) {
                adaptor = new Slf4jLoggerFactoryAdaptor(); // 使用SLF4J适配器
            }
        } catch (Throwable ignore) {
            adaptor = new JdkLoggerFactoryAdaptor(); // 使用JDK适配器
        }
    }

    /**
     * 获取指定类的Logger实例
     * 
     * @param clazz 类对象
     * @return Logger实例
     */
    public static Logger getLogger(Class<?> clazz) {
        return adaptor.getLogger(clazz);
    }

    /**
     * 获取指定名称的Logger实例
     * 
     * @param name 日志记录器名称
     * @return Logger实例
     */
    public static Logger getLogger(String name) {
        return adaptor.getLogger(name);
    }

    /**
     * 获取当前的日志适配器
     * 
     * @return LoggerFactoryAdaptor实例
     */
    public static LoggerFactoryAdaptor currentAdaptor() {
        return adaptor;
    }

    /**
     * 设置日志适配器
     * 
     * @param adaptor LoggerFactoryAdaptor实例
     */
    public static void setAdaptor(LoggerFactoryAdaptor adaptor) {
        LoggerFactory.adaptor = adaptor;
    }

    private LoggerFactory() {
    }
}
