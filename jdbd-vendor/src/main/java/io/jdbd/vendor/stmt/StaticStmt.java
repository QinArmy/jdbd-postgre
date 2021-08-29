package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultState;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;
import java.util.function.Function;

public interface StaticStmt extends Stmt {

    String getSql();

    Consumer<ResultState> getStatusConsumer();

    @Nullable
    default Function<String, Publisher<byte[]>> getImportFunction() {
        return null;
    }

    @Nullable
    default Function<String, Subscriber<byte[]>> getExportSubscriber() {
        return null;
    }


}
