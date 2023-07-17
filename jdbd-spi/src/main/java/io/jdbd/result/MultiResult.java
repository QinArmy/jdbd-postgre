package io.jdbd.result;


import org.reactivestreams.Publisher;

import java.util.function.Consumer;
import java.util.function.Function;

public interface MultiResult {


    /**
     * @return A Reactive Streams {@link Publisher} with rx operators that emits 0 to 1 elements
     * ,like {@code reactor.core.publisher.Mono}.
     * @throws NoMoreResultException emit when {@link MultiResult} end and no buffer.
     */
    Publisher< ResultStates> nextUpdate();


    /**
     * @return A Reactive Streams {@link Publisher} with rx operators that emits 0 to N elements
     * ,like {@code reactor.core.publisher.Flux}.
     */


    <R> Publisher<R> nextQuery(Function<CurrentRow, R> function);


    <R> Publisher<R> nextQuery(Function<CurrentRow, R> function, Consumer<ResultStates> consumer);


}
