package io.jdbd;


import org.reactivestreams.Publisher;

import java.util.function.Consumer;

public interface MultiResults {

    /**
     * @throws NoMoreResultException  emit when {@link MultiResults} end and no buffer.
     * @throws TooManyResultException emit when database return result set count more than expect
     */
    Publisher<ResultStates> nextUpdate();

    Publisher<ResultRow> nextQuery(Consumer<ResultStates> statesConsumer);


}
