package com.berry.clickhouse.tcp.client.misc;

import java.util.function.Function;

/**
 * Either接口表示一个值可以是左侧或右侧的类型
 * 
 * @param <L> 左侧值类型
 * @param <R> 右侧值类型
 */
public interface Either<L, R> {

    /**
     * 创建左侧值
     * 
     * @param value 左侧值
     * @return 左侧Either实例
     */
    static <L1, R1> Either<L1, R1> left(L1 value) {
        return new Left<>(value);
    }

    /**
     * 创建右侧值
     * 
     * @param value 右侧值
     * @return 右侧Either实例
     */
    static <L1, R1> Either<L1, R1> right(R1 value) {
        return new Right<>(value);
    }

    boolean isRight();

    <R1> Either<L, R1> map(Function<R, R1> f);

    <R1> Either<L, R1> flatMap(Function<R, Either<L, R1>> f);

    L getLeft();

    R getRight();
}
