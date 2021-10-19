package io.jdbd.vendor.stmt;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.util.function.Function;

public interface StaticMultiStmt extends BatchStmt {

    String getMultiStmt();

    /**
     * @return always 0 .
     */
    @Override
    int getFetchSize();

    /**
     * @return always null.
     */
    @Nullable
    @Override
    Function<Object, Publisher<byte[]>> getImportPublisher();

    /**
     * @return always null.
     */
    @Nullable
    @Override
    Function<Object, Subscriber<byte[]>> getExportSubscriber();


}
