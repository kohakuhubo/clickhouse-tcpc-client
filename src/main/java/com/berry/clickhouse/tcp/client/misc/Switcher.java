package com.berry.clickhouse.tcp.client.misc;

/**
 * Switcher类用于在两个值之间切换
 * 
 * @param <T> 值的类型
 */
public class Switcher<T> {
    private final T left; // 左侧值
    private final T right; // 右侧值

    private boolean isRight = true; // 当前选择的值

    /**
     * 构造函数，初始化Switcher
     * 
     * @param left 左侧值
     * @param right 右侧值
     */
    public Switcher(T left, T right) {
        this.left = left;
        this.right = right;
    }

    /**
     * 选择当前值
     * 
     * @param isRight 如果为true则选择右侧值
     */
    public void select(boolean isRight) {
        this.isRight = isRight; // 设置当前选择
    }

    /**
     * 获取当前选择的值
     * 
     * @return 当前值
     */
    public T get() {
        return this.isRight ? right : left; // 返回当前选择的值
    }
}
