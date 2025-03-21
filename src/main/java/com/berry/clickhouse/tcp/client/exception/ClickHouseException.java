package com.berry.clickhouse.tcp.client.exception;

public class ClickHouseException extends RuntimeException {

    protected int errCode;

    public ClickHouseException(int errCode, String message) {
        super(message);
        this.errCode = errCode;
    }

    public ClickHouseException(int errCode, String message, Throwable cause) {
        super(message, cause);
        this.errCode = errCode;
    }

    public ClickHouseException(int errCode, Throwable cause) {
        super(cause);
        this.errCode = errCode;
    }

    public int errCode() {
        return errCode;
    }
}
