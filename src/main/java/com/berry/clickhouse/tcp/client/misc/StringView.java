package com.berry.clickhouse.tcp.client.misc;

/**
 * StringView类实现了CharSequence接口
 * 用于表示字符串的子序列
 */
public class StringView implements CharSequence {

    private final String str; // 原始字符串

    private final int start; // 开始索引

    private final int end; // 结束索引

    private int hashVal; // 哈希值

    private String repr; // 字符串表示

    /**
     * 构造函数，初始化StringView
     * 
     * @param str 原始字符串
     * @param start 开始索引
     * @param end 结束索引
     */
    public StringView(String str, int start, int end) {
        this.str = str;
        this.start = Math.max(start, 0);
        this.end = Math.min(end, str.length());
    }

    @Override
    public int length() {
        return end - start; // 返回长度
    }

    @Override
    public char charAt(int index) {
        return str.charAt(start + index); // 返回指定索引的字符
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return subview(start, end); // 返回子序列
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StringView)) {
            return false; // 检查是否为StringView实例
        }
        StringView other = (StringView) obj;
        int len = length();
        if (len != other.length()) {
            return false; // 长度不相等
        }
        int start1 = start;
        int start2 = other.start;
        while (start1 < end && start2 < other.end) {
            if (str.charAt(start1) != other.str.charAt(start2)) {
                return false; // 字符不相等
            }
            start1++;
            start2++;
        }
        return true; // 相等
    }

    @Override
    public int hashCode() {
        if (hashVal == 0) {
            int h = 0x01000193;
            for (int i = start; i < end; i++) {
                h = 31 * h + str.charAt(i); // 计算哈希值
            }
            if (h == 0) {
                h++;
            }
            hashVal = h; // 更新哈希值
        }
        return hashVal; // 返回哈希值
    }

    @Override
    public String toString() {
        return repr(); // 返回字符串表示
    }

    public String data() {
        return str; // 返回原始字符串
    }

    public int start() {
        return this.start; // 返回开始索引
    }

    public int end() {
        return this.end; // 返回结束索引
    }

    public StringView subview(int start0, int end0) {
        return new StringView(str, start + start0, start + end0); // 返回子视图
    }

    public String repr() {
        if (repr == null) {
            repr = str.substring(this.start, this.end); // 计算字符串表示
        }
        return repr; // 返回字符串表示
    }

    public boolean checkEquals(String expectString) {
        if (expectString == null || expectString.length() != end - start)
            return false; // 检查长度
        for (int i = 0; i < expectString.length(); i++) {
            if (expectString.charAt(i) != str.charAt(start + i))
                return false; // 字符不相等
        }
        return true; // 相等
    }

    public boolean checkEqualsIgnoreCase(String expectString) {
        if (expectString == null || expectString.length() != end - start)
            return false; // 检查长度
        for (int i = 0; i < expectString.length(); i++) {
            if (expectString.charAt(i) != str.charAt(start + i))
                return false; // 字符不相等
        }
        return true; // 相等
    }
}
