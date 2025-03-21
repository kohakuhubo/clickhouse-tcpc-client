package com.berry.clickhouse.tcp.client.misc;


import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

public class Slice implements Iterable<Object> {
    private Object[] array;

    private int capacity;
    private int offset;
    private int pos;

    public Slice(int capacity) {
        this.offset = 0;
        this.pos = 0;

        this.array = new Object[capacity];
        this.capacity = capacity;
    }

    public Slice(Object[] array) {
        this.offset = 0;
        this.pos = array.length;

        this.array = array;
        this.capacity = array.length;
    }

    public Slice(Slice slice, int offset, int pos) {
        this.array = slice.array;

        this.capacity = slice.capacity;
        this.offset = offset;
        this.pos = pos;
    }


    public int size() {
        return pos - offset;
    }

    public Slice sub(int offsetAdd, int posAdd) {
        return new Slice(this, offset + offsetAdd, offset + posAdd);
    }

    public Object get(int index) {
        return array[index + offset];
    }

    public void add(Object object) {
        if (pos >= capacity) {
            grow(capacity);
        }
        array[pos] = object;
        pos++;
    }

    public void set(int index, Object object) {
        array[offset + index] = object;
    }


    private void grow(int minCapacity) {
        int oldCapacity = array.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        array = Arrays.copyOf(array, newCapacity);
        capacity = newCapacity;
    }

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0)
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ?
                Integer.MAX_VALUE :
                MAX_ARRAY_SIZE;
    }

    public static class SliceIterator implements Iterator<Object> {
        int current;
        Slice slice;

        SliceIterator(Slice slice) {
            this.slice = slice;
            this.current = slice.offset;
        }

        @Override
        public boolean hasNext() {
            return current < slice.pos;
        }

        @Override
        public Object next() {
            Object obj = slice.array[current];
            current++;
            return obj;
        }

        @Override
        public void remove() {
        }

        @Override
        public void forEachRemaining(Consumer<? super Object> action) {
        }
    }

    
    @Override
    public Iterator<Object> iterator() {
        return new SliceIterator(this);
    }

    @Override
    public void forEach(Consumer<? super Object> action) {

    }

    @Override
    public Spliterator<Object> spliterator() {
        return null;
    }
}
