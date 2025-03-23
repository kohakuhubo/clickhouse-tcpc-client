package com.berry.clickhouse.tcp.client.misc;

import java.sql.SQLException;

/**
 * SQLLexer类用于解析SQL语句
 * 提供了对SQL语句中不同类型的字面量的解析功能
 */
public class SQLLexer {
    private int pos; // 当前解析位置
    private final String data; // SQL语句

    /**
     * 构造函数，初始化SQLLexer
     * 
     * @param pos 初始位置
     * @param data SQL语句
     */
    public SQLLexer(int pos, String data) {
        this.pos = pos;
        this.data = data;
    }

    /**
     * 获取当前字符并移动到下一个字符
     * 
     * @return 当前字符
     */
    public char character() {
        return eof() ? 0 : data.charAt(pos++);
    }

    /**
     * 解析整数字面量
     * 
     * @return 解析得到的整数
     */
    public int intLiteral() {
        skipAnyWhitespace();

        int start = pos;

        if (isCharacter('-') || isCharacter('+'))
            pos++;

        for (; pos < data.length(); pos++)
            if (!isNumericASCII(data.charAt(pos)))
                break;

        return Integer.parseInt(new StringView(data, start, pos).toString());
    }

    /**
     * 解析数字字面量
     * 
     * @return 解析得到的数字
     */
    public Number numberLiteral() {
        skipAnyWhitespace();

        int start = pos;
        boolean isHex = false;
        boolean isBinary = false;
        boolean isDouble = false;
        boolean hasExponent = false;
        boolean hasSigned = false;

        if (isCharacter('-') || isCharacter('+')) {
            hasSigned = true;
            pos++;
        }

        if (pos + 2 < data.length()) {
            if (data.charAt(pos) == '0' && (data.charAt(pos + 1) == 'x' || data.charAt(pos + 1) == 'X'
                    || data.charAt(pos + 1) == 'b' || data.charAt(pos + 1) == 'B')) {
                isHex = data.charAt(pos + 1) == 'x' || data.charAt(pos + 1) == 'X';
                isBinary = data.charAt(pos + 1) == 'b' || data.charAt(pos + 1) == 'B';
                pos += 2;
            }
        }

        for (; pos < data.length(); pos++) {
            if (isHex ? !isHexDigit(data.charAt(pos)) : !isNumericASCII(data.charAt(pos))) {
                break;
            }
        }

        if (pos < data.length() && data.charAt(pos) == '.') {
            isDouble = true;
            for (pos++; pos < data.length(); pos++) {
                if (isHex ? !isHexDigit(data.charAt(pos)) : !isNumericASCII(data.charAt(pos)))
                    break;
            }
        }

        if (pos + 1 < data.length()
                && (isHex ? (data.charAt(pos) == 'p' || data.charAt(pos) == 'P')
                        : (data.charAt(pos) == 'e' || data.charAt(pos) == 'E'))) {
            hasExponent = true;
            pos++;

            if (pos + 1 < data.length() && (data.charAt(pos) == '-' || data.charAt(pos) == '+')) {
                pos++;
            }

            for (; pos < data.length(); pos++) {
                char ch = data.charAt(pos);
                if (!isNumericASCII(ch)) {
                    break;
                }
            }
        }

        if (isBinary) {
            String signed = hasSigned ? data.charAt(start) + "" : "";
            int begin = start + (hasSigned ? 3 : 2);
            return Long.parseLong(signed + new StringView(data, begin, pos).toString(), 2);
        } else if (isDouble || hasExponent) {
            return Double.valueOf(new StringView(data, start, pos).toString());
        } else if (isHex) {
            String signed = hasSigned ? data.charAt(start) + "" : "";
            int begin = start + (hasSigned ? 3 : 2);
            return Long.parseLong(signed + new StringView(data, begin, pos), 16);
        } else {
            return Long.parseLong(new StringView(data, start, pos).toString());
        }
    }

    /**
     * 解析字符串字面量
     * 
     * @return 解析得到的字符串
     * @throws SQLException 如果解析失败
     */
    public String stringLiteral() throws SQLException {
        return stringView().toString();
    }

    /**
     * 获取字符串视图
     * 
     * @return 字符串视图
     * @throws SQLException 如果解析失败
     */
    public StringView stringView() throws SQLException {
        skipAnyWhitespace();
        Validate.isTrue(isCharacter('\''));
        return stringLiteralWithQuoted('\'');
    }

    /**
     * 检查是否到达文件末尾
     * 
     * @return 如果到达文件末尾则返回true
     */
    public boolean eof() {
        skipAnyWhitespace();
        return pos >= data.length();
    }

    /**
     * 检查当前字符是否与给定字符相同
     * 
     * @param ch 要检查的字符
     * @return 如果相同则返回true
     */
    public boolean isCharacter(char ch) {
        return !eof() && data.charAt(pos) == ch;
    }

    /**
     * 获取裸单词
     * 
     * @return 字符串视图
     * @throws SQLException 如果解析失败
     */
    public StringView bareWord() throws SQLException {
        skipAnyWhitespace();
        if (isCharacter('`')) {
            return stringLiteralWithQuoted('`');
        } else if (isCharacter('"')) {
            return stringLiteralWithQuoted('"');
        } else if (data.charAt(pos) == '_'
                || (data.charAt(pos) >= 'a' && data.charAt(pos) <= 'z')
                || (data.charAt(pos) >= 'A' && data.charAt(pos) <= 'Z')) {
            int start = pos;
            for (pos++; pos < data.length(); pos++) {
                if (!('_' == data.charAt(pos)
                        || (data.charAt(pos) >= 'a' && data.charAt(pos) <= 'z')
                        || (data.charAt(pos) >= 'A' && data.charAt(pos) <= 'Z')
                        || (data.charAt(pos) >= '0' && data.charAt(pos) <= '9'))) {
                    break;
                }
            }
            return new StringView(data, start, pos);
        }
        throw new SQLException("Expect Bare Token.");
    }

    /**
     * 检查当前字符是否为空白字符
     * 
     * @return 如果是空白字符则返回true
     */
    public boolean isWhitespace() {
        return data.charAt(pos++) == ' ';
    }

    private boolean isNumericASCII(char c) {
        return c >= '0' && c <= '9';
    }

    private boolean isHexDigit(char c) {
        return isNumericASCII(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    private void skipAnyWhitespace() {
        for (; pos < data.length(); pos++) {
            if (data.charAt(pos) != ' '
                    && data.charAt(pos) != '\t'
                    && data.charAt(pos) != '\n'
                    && data.charAt(pos) != '\r'
                    && data.charAt(pos) != '\f') {
                return;
            }
        }
    }

    private StringView stringLiteralWithQuoted(char quoted) throws SQLException {
        int start = pos;
        Validate.isTrue(data.charAt(pos) == quoted);
        for (pos++; pos < data.length(); pos++) {
            if (data.charAt(pos) == '\\')
                pos++;
            else if (data.charAt(pos) == quoted)
                return new StringView(data, start + 1, pos++);
        }
        throw new SQLException("The String Literal is no Closed.");
    }
}
