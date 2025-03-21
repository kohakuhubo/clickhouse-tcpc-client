package com.berry.clickhouse.tcp.client.stream;

public interface InputFormat<T, E extends Throwable> {

    String name();

    void fill(T payload) throws E;
}
