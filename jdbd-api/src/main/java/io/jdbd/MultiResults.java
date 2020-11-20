package io.jdbd;


import org.reactivestreams.Publisher;

import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface MultiResults {

    Publisher<Long> nextUpdate(Consumer<ResultStates> statesConsumer);

    <T> Publisher<T> nextQuery(BiFunction<ResultRow, ResultRowMeta, T> rowDecoder, Consumer<ResultStates> statesConsumer);

}
