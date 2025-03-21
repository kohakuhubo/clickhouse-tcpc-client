package com.berry.clickhouse.tcp.client.misc;

import com.berry.clickhouse.tcp.client.exception.InvalidOperationException;

import java.util.Locale;
import java.util.function.Function;

public class Right<L, R> implements Either<L, R> {

    private final R value;

    Right(R value) {
        this.value = value;
    }

    @Override
    public boolean isRight() {
        return true;
    }

    @Override
    public <R1> Either<L, R1> map(Function<R, R1> f) {
        return Either.right(f.apply(value));
    }

    @Override
    public <R1> Either<L, R1> flatMap(Function<R, Either<L, R1>> f) {
        return f.apply(value);
    }

    @Override
    public L getLeft() {
        throw new InvalidOperationException("Right not support #getLeft");
    }

    @Override
    public R getRight() {
        return value;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "Right(%s)", value);
    }
}
