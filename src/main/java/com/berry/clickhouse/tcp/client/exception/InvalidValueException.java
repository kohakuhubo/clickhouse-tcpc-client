package com.berry.clickhouse.tcp.client.exception;

public class InvalidValueException extends ClickHouseClientException {

    public InvalidValueException(String message) {
        super(message);
    }

    public InvalidValueException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidValueException(Throwable cause) {
        super(cause);
    }
}
