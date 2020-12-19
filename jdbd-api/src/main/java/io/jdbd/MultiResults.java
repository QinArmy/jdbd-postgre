package io.jdbd;


import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface MultiResults {

    /**
     * @throws NoMoreResultException  emit when {@link MultiResults} end and no buffer.
     * @throws TooManyResultException emit when database return result set count more than expect
     */
    Mono<ResultStates> nextUpdate();


    /**
     * @throws NoMoreResultException  emit when {@link MultiResults} end and no buffer.
     * @throws TooManyResultException emit when database return result set count more than expect
     */
    <T> Flux<T> nextQuery(BiFunction<ResultRow, ResultRowMeta, T> rowDecoder, Consumer<ResultStates> statesConsumer);


}
