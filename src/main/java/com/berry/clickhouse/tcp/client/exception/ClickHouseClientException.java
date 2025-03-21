package com.berry.clickhouse.tcp.client.exception;

import com.berry.clickhouse.tcp.client.settings.ClickHouseErrCode;

public class ClickHouseClientException extends ClickHouseException {

    public ClickHouseClientException(String message) {
        super(ClickHouseErrCode.CLIENT_ERROR.code(), message);
    }

    public ClickHouseClientException(String message, Throwable cause) {
        super(ClickHouseErrCode.CLIENT_ERROR.code(), message, cause);
    }

    public ClickHouseClientException(Throwable cause) {
        super(ClickHouseErrCode.CLIENT_ERROR.code(), cause);
    }
}
