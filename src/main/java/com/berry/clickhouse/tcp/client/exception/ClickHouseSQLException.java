package com.berry.clickhouse.tcp.client.exception;

import java.sql.SQLException;

public class ClickHouseSQLException extends SQLException {

    public ClickHouseSQLException(int code, String message) {
        this(code, message, null);
    }

    public ClickHouseSQLException(int code, String message, Throwable cause) {
        super(message, null, code, cause);
    }
}
