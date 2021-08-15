package io.jdbd.result;

import org.reactivestreams.Publisher;

import java.util.function.Consumer;

/**
 * @see MultiResult
 */
@Deprecated
public interface SingleResult {

    boolean isQuery();

    int getIndex();

    /**
     * @return A Reactive Streams {@link Publisher} with rx operators that emits 0 to 1 elements
     * ,like {@code reactor.core.publisher.Mono}.
     */
    Publisher<ResultState> receiveUpdate();

    /**
     * @return A Reactive Streams {@link Publisher} with rx operators that emits 0 to N elements
     * ,like {@code reactor.core.publisher.Flux}.
     */
    Publisher<ResultRow> receiveQuery(Consumer<ResultState> statesConsumer);

    /**
     * @see #receiveQuery(Consumer)
     */
    Publisher<ResultRow> receiveQuery();


}
