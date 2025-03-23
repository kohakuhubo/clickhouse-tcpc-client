package com.berry.clickhouse.tcp.client.log;

/**
 * Logging接口定义了获取Logger实例的方法
 * 提供了日志记录的基本功能
 */
public interface Logging {

    /**
     * 获取Logger实例
     * 
     * @return Logger实例
     */
    Logger logger();
}
