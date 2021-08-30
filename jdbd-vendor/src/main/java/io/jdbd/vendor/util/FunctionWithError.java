package io.jdbd.vendor.util;

import java.util.function.Function;

/**
 * Can't use {@link Function},because need throw {@link Exception}
 */
@FunctionalInterface
public interface FunctionWithError<T, R> {

    R apply(T t) throws Exception;
}
