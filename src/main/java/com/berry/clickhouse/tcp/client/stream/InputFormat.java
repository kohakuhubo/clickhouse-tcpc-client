package com.berry.clickhouse.tcp.client.stream;

/**
 * InputFormat接口定义了输入格式的基本操作
 * 包括获取格式名称和填充数据的方法
 */
public interface InputFormat<T, E extends Throwable> {

    /**
     * 获取输入格式的名称
     * 
     * @return 输入格式名称
     */
    String name();

    /**
     * 填充数据到指定的负载中
     * 
     * @param payload 负载
     * @throws E 可能抛出的异常
     */
    void fill(T payload) throws E;
}
