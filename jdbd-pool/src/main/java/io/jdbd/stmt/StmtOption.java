package io.jdbd.stmt;

import io.jdbd.lang.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

/**
 * <p>
 * This interface representing option of {@link io.jdbd.statement.Statement},
 * and is used by  the implementation of {@link Stmt} .
 * </p>
 */
public interface StmtOption {

    int getTimeout();

    int getFetchSize();

    /**
     * <p>
     * {@link Stmt} implementation must invoke this method,because this method implementation like below:
     * <pre>
     *          final Function&lt;Object, Publisher&lt;byte[]>> function = this.importPublisher;
     *         if (function != null) {
     *             this.importPublisher = n1ull;
     *         }
     *         return function;
     *      </pre>
     * </p>
     */
    @Nullable
    Function<Object, Publisher<byte[]>> getImportPublisher();

    /**
     * <p>
     * {@link Stmt} implementation must invoke this method,because this method implementation like below:
     * <pre>
     *           final Function&lt;Object, Subscriber&lt;byte[]>> function = this.exportPublisher;
     *         if (function != null) {
     *             this.exportPublisher = null;
     *         }
     *         return function;
     *      </pre>
     * </p>
     */
    @Nullable
    Function<Object, Subscriber<byte[]>> getExportSubscriber();

}
