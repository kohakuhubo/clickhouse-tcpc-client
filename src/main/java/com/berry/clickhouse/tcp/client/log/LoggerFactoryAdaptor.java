package com.berry.clickhouse.tcp.client.log;

public interface LoggerFactoryAdaptor {

    Logger getLogger(String name);

    default Logger getLogger(Class<?> clazz) {
        return getLogger(clazz.getName());
    }

}
