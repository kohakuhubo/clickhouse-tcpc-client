package com.berry.clickhouse.tcp.client.misc;

import com.berry.clickhouse.tcp.client.exception.InvalidOperationException;

import java.util.Locale;
import java.util.function.Function;

/**
 * Left类实现了Either接口的左侧值
 * 
 * @param <L> 左侧值类型
 * @param <R> 右侧值类型
 */
public class Left<L, R> implements Either<L, R> {

    private final L value; // 左侧值

    /**
     * 构造函数，初始化Left
     * 
     * @param value 左侧值
     */
    Left(L value) {
        this.value = value;
    }

    @Override
    public boolean isRight() {
        return false; // 返回false表示是左侧值
    }

    @Override
    public <R1> Either<L, R1> map(Function<R, R1> f) {
        return Either.left(value); // 返回左侧值
    }

    @Override
    public <R1> Either<L, R1> flatMap(Function<R, Either<L, R1>> f) {
        return Either.left(value); // 返回左侧值
    }

    @Override
    public L getLeft() {
        return value; // 返回左侧值
    }

    @Override
    public R getRight() {
        throw new InvalidOperationException("Left not support #getRight"); // 不支持获取右侧值
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "Left(%s)", value); // 返回字符串表示
    }
}
