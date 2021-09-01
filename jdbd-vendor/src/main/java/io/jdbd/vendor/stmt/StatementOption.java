package io.jdbd.vendor.stmt;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.util.function.Function;

public interface StatementOption {

    int getTimeout();

    int getFetchSize();

    @Nullable
    Function<Object, Publisher<byte[]>> getImportFunction();

    @Nullable
    Function<Object, Subscriber<byte[]>> getExportSubscriber();

}
