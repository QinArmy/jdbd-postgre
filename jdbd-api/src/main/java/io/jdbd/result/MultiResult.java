package io.jdbd.result;


import io.jdbd.stmt.TooManyResultException;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;

public interface MultiResult {

    Consumer<ResultState> EMPTY_CONSUMER = resultStates -> {
    };


    /**
     * @return A Reactive Streams {@link Publisher} with rx operators that emits 0 to 1 elements
     * ,like {@code reactor.core.publisher.Mono}.
     * @throws NoMoreResultException  emit when {@link MultiResult} end and no buffer.
     * @throws TooManyResultException emit when database return result set count more than expect
     */
    Publisher<ResultState> nextUpdate();

    /**
     * @return A Reactive Streams {@link Publisher} with rx operators that emits 0 to N elements
     * ,like {@code reactor.core.publisher.Flux}.
     */
    Publisher<ResultRow> nextQuery(Consumer<ResultState> statesConsumer);

    /**
     * @see #nextQuery(Consumer)
     */
    Publisher<ResultRow> nextQuery();


}
