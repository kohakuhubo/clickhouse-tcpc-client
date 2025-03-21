package com.berry.clickhouse.tcp.client.misc;

@FunctionalInterface
public interface CheckedSupplier<R, E extends Throwable> {

    R get() throws E;
}
