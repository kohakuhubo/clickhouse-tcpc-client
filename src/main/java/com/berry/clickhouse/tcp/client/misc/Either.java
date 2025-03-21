package com.berry.clickhouse.tcp.client.misc;

import java.util.function.Function;

public interface Either<L, R> {

    static <L1, R1> Either<L1, R1> left(L1 value) {
        return new Left<>(value);
    }

    static <L1, R1> Either<L1, R1> right(R1 value) {
        return new Right<>(value);
    }

    boolean isRight();

    <R1> Either<L, R1> map(Function<R, R1> f);

    <R1> Either<L, R1> flatMap(Function<R, Either<L, R1>> f);

    L getLeft();

    R getRight();
}
