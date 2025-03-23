package com.berry.clickhouse.tcp.client.misc;

import com.berry.clickhouse.tcp.client.exception.InvalidOperationException;

import java.util.Locale;
import java.util.function.Function;

/**
 * Right类实现了Either接口的右侧值
 * 
 * @param <L> 左侧值类型
 * @param <R> 右侧值类型
 */
public class Right<L, R> implements Either<L, R> {

    private final R value; // 右侧值

    /**
     * 构造函数，初始化Right
     * 
     * @param value 右侧值
     */
    Right(R value) {
        this.value = value;
    }

    @Override
    public boolean isRight() {
        return true; // 返回true表示是右侧值
    }

    @Override
    public <R1> Either<L, R1> map(Function<R, R1> f) {
        return Either.right(f.apply(value)); // 返回右侧值
    }

    @Override
    public <R1> Either<L, R1> flatMap(Function<R, Either<L, R1>> f) {
        return f.apply(value); // 返回右侧值
    }

    @Override
    public L getLeft() {
        throw new InvalidOperationException("Right not support #getLeft"); // 不支持获取左侧值
    }

    @Override
    public R getRight() {
        return value; // 返回右侧值
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "Right(%s)", value); // 返回字符串表示
    }
}
