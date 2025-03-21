package com.berry.clickhouse.tcp.client.log;

public class JdkLoggerFactoryAdaptor implements LoggerFactoryAdaptor {

    @Override
    public Logger getLogger(String name) {
        return new JdkLogger(java.util.logging.Logger.getLogger(name));
    }
}
