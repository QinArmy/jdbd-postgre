package io.jdbd.vendor.stmt;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.util.function.Function;

/**
 * <p>
 * This interface representing object that wrap sql and parameter and option(eg: timeout).
 * The implementation of this interface is used by the implementation of {@link io.jdbd.stmt.Statement}
 * </p>
 * <p>
 * This interface is a base interface of :
 *     <ul>
 *         <li>{@link io.jdbd.vendor.stmt.StaticStmt}</li>
 *         <li>{@link io.jdbd.vendor.stmt.StaticBatchStmt}</li>
 *         <li>{@link io.jdbd.vendor.stmt.ParamStmt}</li>
 *         <li>{@link io.jdbd.vendor.stmt.ParamBatchStmt}</li>
 *         <li>{@link io.jdbd.vendor.stmt.ParamMultiStmt}</li>
 *     </ul>
 * </p>
 * </p>
 */
public interface Stmt {

    int getTimeout();

    int getFetchSize();

    @Nullable
    Function<Object, Publisher<byte[]>> getImportPublisher();

    @Nullable
    Function<Object, Subscriber<byte[]>> getExportSubscriber();


}
