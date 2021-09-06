package io.jdbd.vendor.stmt;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.util.function.Function;

/**
 * <p>
 * This interface representing option of {@link io.jdbd.stmt.Statement},
 * and is used by  the implementation of {@link Stmt} .
 * </p>
 */
public interface StatementOption {

    int getTimeout();

    int getFetchSize();

    @Nullable
    Function<Object, Publisher<byte[]>> getImportFunction();

    @Nullable
    Function<Object, Subscriber<byte[]>> getExportSubscriber();

}
