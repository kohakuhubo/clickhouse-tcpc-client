package com.berry.clickhouse.tcp.client.misc;

import com.berry.clickhouse.tcp.client.exception.InvalidValueException;

import java.sql.SQLException;

/**
 * Validate类提供了一些用于验证条件的方法
 * 如果条件不满足，将抛出相应的异常
 */
public class Validate {

    /**
     * 确保表达式为真
     * 
     * @param expr 表达式
     */
    public static void ensure(boolean expr) {
        ensure(expr, "");
    }

    /**
     * 确保表达式为真，并提供错误信息
     * 
     * @param expr 表达式
     * @param message 错误信息
     */
    public static void ensure(boolean expr, String message) {
        if (!expr) {
            throw new InvalidValueException(message); // 抛出无效值异常
        }
    }

    /**
     * 确保表达式为真，如果不为真则抛出SQLException
     * 
     * @param expression 表达式
     * @throws SQLException 如果表达式不为真
     */
    public static void isTrue(boolean expression) throws SQLException {
        isTrue(expression, null);
    }

    /**
     * 确保表达式为真，并提供错误信息
     * 
     * @param expression 表达式
     * @param message 错误信息
     * @throws SQLException 如果表达式不为真
     */
    public static void isTrue(boolean expression, String message) throws SQLException {
        if (!expression) {
            throw new SQLException(message); // 抛出SQLException
        }
    }
}
