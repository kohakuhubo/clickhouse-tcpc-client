package com.berry.clickhouse.tcp.client.data;

import java.util.concurrent.ConcurrentLinkedDeque;

public class LimitedConcurrentLinkedDeque extends ConcurrentLinkedDeque {

    private final int capacity;

    public LimitedConcurrentLinkedDeque(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public boolean add(Object o) {
        return super.add(o);
    }
}
