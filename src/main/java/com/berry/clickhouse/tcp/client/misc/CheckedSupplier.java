package com.berry.clickhouse.tcp.client.misc;

/**
 * CheckedSupplier接口用于定义带有异常的供应者
 * 
 * @param <R> 返回类型
 * @param <E> 异常类型
 */
@FunctionalInterface
public interface CheckedSupplier<R, E extends Throwable> {

    /**
     * 获取结果
     * 
     * @return 结果
     * @throws E 可能抛出的异常
     */
    R get() throws E;
}
