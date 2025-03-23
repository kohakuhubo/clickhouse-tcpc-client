package com.berry.clickhouse.tcp.client.log;

/**
 * LoggerFactoryAdaptor接口定义了日志适配器的基本操作
 * 提供获取Logger实例的方法
 */
public interface LoggerFactoryAdaptor {

    /**
     * 获取指定名称的Logger实例
     * 
     * @param name 日志记录器名称
     * @return Logger实例
     */
    Logger getLogger(String name);

    /**
     * 获取指定类的Logger实例
     * 
     * @param clazz 类对象
     * @return Logger实例
     */
    default Logger getLogger(Class<?> clazz) {
        return getLogger(clazz.getName());
    }

}
