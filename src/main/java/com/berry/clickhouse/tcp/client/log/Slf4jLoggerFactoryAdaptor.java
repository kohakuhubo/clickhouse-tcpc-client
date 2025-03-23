package com.berry.clickhouse.tcp.client.log;

/**
 * Slf4jLoggerFactoryAdaptor类实现LoggerFactoryAdaptor接口
 * 用于创建Slf4jLogger实例
 */
public class Slf4jLoggerFactoryAdaptor implements LoggerFactoryAdaptor {

    /**
     * 获取指定名称的Logger实例
     * 
     * @param name 日志记录器名称
     * @return Slf4jLogger实例
     */
    @Override
    public Logger getLogger(String name) {
        return new Slf4jLogger(org.slf4j.LoggerFactory.getLogger(name));
    }
}
