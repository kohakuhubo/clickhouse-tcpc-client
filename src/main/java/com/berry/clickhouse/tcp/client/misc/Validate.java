package com.berry.clickhouse.tcp.client.misc;

import com.berry.clickhouse.tcp.client.exception.InvalidValueException;


import java.sql.SQLException;

public class Validate {

    public static void ensure(boolean expr) {
        ensure(expr, "");
    }

    public static void ensure(boolean expr, String message) {
        if (!expr) {
            throw new InvalidValueException(message);
        }
    }

    public static void isTrue(boolean expression) throws SQLException {
        isTrue(expression, null);
    }

    public static void isTrue(boolean expression,  String message) throws SQLException {
        if (!expression) {
            throw new SQLException(message);
        }
    }
}
