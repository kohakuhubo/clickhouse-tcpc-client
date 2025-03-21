package com.berry.clickhouse.tcp.client.log;

public class Slf4jLoggerFactoryAdaptor implements LoggerFactoryAdaptor {

    @Override
    public Logger getLogger(String name) {
        return new Slf4jLogger(org.slf4j.LoggerFactory.getLogger(name));
    }
}
