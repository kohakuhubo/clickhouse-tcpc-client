package com.berry.clickhouse.tcp.client.misc;

/**
 * CheckedIterator接口定义了带有异常的迭代器
 * 
 * @param <T> 元素类型
 * @param <E> 异常类型
 */
public interface CheckedIterator<T, E extends Throwable> {

    /**
     * 检查是否还有下一个元素
     * 
     * @return 如果有下一个元素则返回true
     * @throws E 可能抛出的异常
     */
    boolean hasNext() throws E;

    /**
     * 获取下一个元素
     * 
     * @return 下一个元素
     * @throws E 可能抛出的异常
     */
    T next() throws E;
}
