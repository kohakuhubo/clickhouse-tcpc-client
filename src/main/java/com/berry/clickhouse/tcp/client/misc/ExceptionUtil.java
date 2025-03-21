package com.berry.clickhouse.tcp.client.misc;

import com.berry.clickhouse.tcp.client.exception.ClickHouseException;
import com.berry.clickhouse.tcp.client.exception.ClickHouseSQLException;
import com.berry.clickhouse.tcp.client.settings.ClickHouseErrCode;


import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class ExceptionUtil {

    public static RuntimeException unchecked(Exception checked) {
        return new RuntimeException(checked);
    }

    public static <T, R> Function<T, R> unchecked(CheckedFunction<T, R> checked) {
        return t -> {
            try {
                return checked.apply(t);
            } catch (Exception rethrow) {
                throw unchecked(rethrow);
            }
        };
    }

    public static <T, U, R> BiFunction<T, U, R> unchecked(CheckedBiFunction<T, U, R> checked) {
        return (t, u) -> {
            try {
                return checked.apply(t, u);
            } catch (Exception rethrow) {
                throw unchecked(rethrow);
            }
        };
    }

    public static <T> Supplier<T> unchecked(CheckedSupplier<T> checked) {
        return () -> {
            try {
                return checked.get();
            } catch (Exception rethrow) {
                throw unchecked(rethrow);
            }
        };
    }

    public static void rethrowSQLException(CheckedRunnable checked) throws ClickHouseSQLException {
        try {
            checked.run();
        } catch (Exception rethrow) {
            int errCode = ClickHouseErrCode.UNKNOWN_ERROR.code();
            ClickHouseException ex = ExceptionUtil.recursiveFind(rethrow, ClickHouseException.class);
            if (ex != null)
                errCode = ex.errCode();
            throw new ClickHouseSQLException(errCode, rethrow.getMessage(), rethrow);
        }
    }

    public static <T> T rethrowSQLException(CheckedSupplier<T> checked) throws ClickHouseSQLException {
        try {
            return checked.get();
        } catch (Exception rethrow) {
            int errCode = ClickHouseErrCode.UNKNOWN_ERROR.code();
            ClickHouseException ex = ExceptionUtil.recursiveFind(rethrow, ClickHouseException.class);
            if (ex != null)
                errCode = ex.errCode();
            throw new ClickHouseSQLException(errCode, rethrow.getMessage(), rethrow);
        }
    }

    
    @SuppressWarnings("unchecked")
    public static <T> T recursiveFind(Throwable th, Class<T> expectedClz) {
        Throwable nest = th;
        while (nest != null) {
            if (expectedClz.isAssignableFrom(nest.getClass())) {
                return (T) nest;
            }
            nest = nest.getCause();
        }
        return null;
    }

    @FunctionalInterface
    public interface CheckedFunction<T, R> {
        R apply(T t) throws Exception;
    }

    @FunctionalInterface
    public interface CheckedBiFunction<T, U, R> {
        R apply(T t, U u) throws Exception;
    }

    @FunctionalInterface
    public interface CheckedBiConsumer<T, U> {
        void accept(T t, U u) throws Exception;
    }

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    public interface CheckedSupplier<T> {
        T get() throws Exception;
    }
}
