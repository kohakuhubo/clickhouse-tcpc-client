package com.berry.clickhouse.tcp.client.misc;

public interface CheckedIterator<T, E extends Throwable> {

    boolean hasNext() throws E;

    T next() throws E;
}
