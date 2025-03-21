package com.berry.clickhouse.tcp.client.exception;

public class NoDefaultValueException extends ClickHouseClientException {

    public NoDefaultValueException(String message) {
        super(message);
    }

    public NoDefaultValueException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoDefaultValueException(Throwable cause) {
        super(cause);
    }
}
