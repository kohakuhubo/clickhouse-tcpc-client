package com.berry.clickhouse.tcp.client.log;

/**
 * JdkLoggerFactoryAdaptor类实现LoggerFactoryAdaptor接口
 * 用于创建JdkLogger实例
 */
public class JdkLoggerFactoryAdaptor implements LoggerFactoryAdaptor {

    /**
     * 获取指定名称的Logger实例
     * 
     * @param name 日志记录器名称
     * @return JdkLogger实例
     */
    @Override
    public Logger getLogger(String name) {
        return new JdkLogger(java.util.logging.Logger.getLogger(name));
    }
}
