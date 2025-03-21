package com.berry.clickhouse.tcp.client.exception;

public class NotImplementedException extends ClickHouseClientException {

    public NotImplementedException(String message) {
        super(message);
    }

    public NotImplementedException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotImplementedException(Throwable cause) {
        super(cause);
    }
}
