package com.berry.clickhouse.tcp.client.misc;

import com.berry.clickhouse.tcp.client.exception.InvalidOperationException;

import java.util.Locale;
import java.util.function.Function;

public class Left<L, R> implements Either<L, R> {

    private final L value;

    Left(L value) {
        this.value = value;
    }

    @Override
    public boolean isRight() {
        return false;
    }

    @Override
    public <R1> Either<L, R1> map(Function<R, R1> f) {
        return Either.left(value);
    }

    @Override
    public <R1> Either<L, R1> flatMap(Function<R, Either<L, R1>> f) {
        return Either.left(value);
    }

    @Override
    public L getLeft() {
        return value;
    }

    @Override
    public R getRight() {
        throw new InvalidOperationException("Left not support #getRight");
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "Left(%s)", value);
    }
}
