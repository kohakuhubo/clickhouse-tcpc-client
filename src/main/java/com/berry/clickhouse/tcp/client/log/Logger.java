package com.berry.clickhouse.tcp.client.log;

public interface Logger {

    String getName();

    boolean isTraceEnabled();

    void trace(String format, Object... arguments);

    void trace(String msg, Throwable t);

    boolean isDebugEnabled();

    void debug(String format, Object... arguments);

    void debug(String msg, Throwable t);

    boolean isInfoEnabled();

    void info(String format, Object... arguments);

    void info(String msg, Throwable t);

    boolean isWarnEnabled();

    void warn(String format, Object... arguments);

    void warn(String msg, Throwable t);

    boolean isErrorEnabled();

    void error(String format, Object... arguments);

    void error(String msg, Throwable t);
}
