package com.berry.clickhouse.tcp.client.misc;

public class Switcher<T> {
    private final T left;
    private final T right;

    private boolean isRight = true;

    public Switcher(T left, T right) {
        this.left = left;
        this.right = right;
    }

    public void select(boolean isRight) {
        this.isRight = isRight;
    }

    public T get() {
        return this.isRight ? right : left;
    }
}
