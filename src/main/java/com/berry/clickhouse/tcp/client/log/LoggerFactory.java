package com.berry.clickhouse.tcp.client.log;

public class LoggerFactory {

    private static LoggerFactoryAdaptor adaptor;

    static {
        try {
            if (org.slf4j.LoggerFactory.getILoggerFactory() != null) {
                adaptor = new Slf4jLoggerFactoryAdaptor();
            }
        } catch (Throwable ignore) {
            adaptor = new JdkLoggerFactoryAdaptor();
        }
    }

    public static Logger getLogger(Class<?> clazz) {
        return adaptor.getLogger(clazz);
    }

    public static Logger getLogger(String name) {
        return adaptor.getLogger(name);
    }

    public static LoggerFactoryAdaptor currentAdaptor() {
        return adaptor;
    }

    public static void setAdaptor(LoggerFactoryAdaptor adaptor) {
        LoggerFactory.adaptor = adaptor;
    }

    private LoggerFactory() {
    }
}
