package com.berry.clickhouse.tcp.client.protocol;

import com.berry.clickhouse.tcp.client.exception.ClickHouseSQLException;
import com.berry.clickhouse.tcp.client.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

public class ExceptionResponse implements Response {

    public static SQLException readExceptionFrom(BinaryDeserializer deserializer) throws IOException {
        int code = deserializer.readInt();
        String name = deserializer.readUTF8StringBinary();
        String message = deserializer.readUTF8StringBinary();
        String stackTrace = deserializer.readUTF8StringBinary();

        if (deserializer.readBoolean()) {
            return new ClickHouseSQLException(
                    code, name + message + ". Stack trace:\n\n" + stackTrace, readExceptionFrom(deserializer));
        }

        return new ClickHouseSQLException(code, name + message + ". Stack trace:\n\n" + stackTrace);
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_EXCEPTION;
    }
}
