package com.berry.clickhouse.tcp.client.misc;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Slice类表示一个可变大小的对象数组切片
 * 提供了对数组的基本操作
 */
public class Slice implements Iterable<Object> {
    private Object[] array; // 存储对象的数组

    private int capacity; // 数组容量
    private int offset; // 偏移量
    private int pos; // 当前大小

    /**
     * 构造函数，初始化指定容量的Slice
     * 
     * @param capacity 初始容量
     */
    public Slice(int capacity) {
        this.offset = 0;
        this.pos = 0;

        this.array = new Object[capacity];
        this.capacity = capacity;
    }

    /**
     * 构造函数，使用现有数组初始化Slice
     * 
     * @param array 现有数组
     */
    public Slice(Object[] array) {
        this.offset = 0;
        this.pos = array.length;

        this.array = array;
        this.capacity = array.length;
    }

    /**
     * 构造函数，基于现有Slice创建子Slice
     * 
     * @param slice 原Slice
     * @param offset 偏移量
     * @param pos 当前大小
     */
    public Slice(Slice slice, int offset, int pos) {
        this.array = slice.array;

        this.capacity = slice.capacity;
        this.offset = offset;
        this.pos = pos;
    }

    /**
     * 获取当前Slice的大小
     * 
     * @return 当前大小
     */
    public int size() {
        return pos - offset; // 返回当前大小
    }

    /**
     * 创建子Slice
     * 
     * @param offsetAdd 偏移量增加值
     * @param posAdd 当前大小增加值
     * @return 新的Slice
     */
    public Slice sub(int offsetAdd, int posAdd) {
        return new Slice(this, offset + offsetAdd, offset + posAdd);
    }

    /**
     * 获取指定索引的对象
     * 
     * @param index 索引
     * @return 对象
     */
    public Object get(int index) {
        return array[index + offset]; // 返回指定索引的对象
    }

    /**
     * 添加对象到Slice
     * 
     * @param object 要添加的对象
     */
    public void add(Object object) {
        if (pos >= capacity) {
            grow(capacity); // 扩展容量
        }
        array[pos] = object; // 添加对象
        pos++;
    }

    /**
     * 设置指定索引的对象
     * 
     * @param index 索引
     * @param object 要设置的对象
     */
    public void set(int index, Object object) {
        array[offset + index] = object; // 设置对象
    }

    /**
     * 扩展Slice的容量
     * 
     * @param minCapacity 最小容量
     */
    private void grow(int minCapacity) {
        int oldCapacity = array.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        array = Arrays.copyOf(array, newCapacity); // 扩展数组
        capacity = newCapacity; // 更新容量
    }

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8; // 最大数组大小

    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0)
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ?
                Integer.MAX_VALUE :
                MAX_ARRAY_SIZE;
    }

    /**
     * Slice的迭代器
     */
    public static class SliceIterator implements Iterator<Object> {
        int current; // 当前索引
        Slice slice; // 关联的Slice

        /**
         * 构造函数，初始化SliceIterator
         * 
         * @param slice 关联的Slice
         */
        SliceIterator(Slice slice) {
            this.slice = slice;
            this.current = slice.offset; // 初始化当前索引
        }

        @Override
        public boolean hasNext() {
            return current < slice.pos; // 检查是否还有下一个元素
        }

        @Override
        public Object next() {
            Object obj = slice.array[current]; // 获取当前对象
            current++; // 移动到下一个
            return obj; // 返回对象
        }

        @Override
        public void remove() {
            // 不支持的操作
        }

        @Override
        public void forEachRemaining(Consumer<? super Object> action) {
            // 不支持的操作
        }
    }

    @Override
    public Iterator<Object> iterator() {
        return new SliceIterator(this); // 返回Slice的迭代器
    }

    @Override
    public void forEach(Consumer<? super Object> action) {
        // 不支持的操作
    }

    @Override
    public Spliterator<Object> spliterator() {
        return null; // 不支持的操作
    }
}
