package io.jdbd.result;


import org.reactivestreams.Publisher;


/**
 * <p>
 * <strong>NOTE</strong> : driver don't send message to database server before first subscribing.
 * </p>
 *
 * @since 1.0
 */
public interface MultiResult extends MultiResultSpec {


    /**
     * @return A Reactive Streams {@link Publisher} with rx operators that emits 0 to 1 elements
     * ,like {@code reactor.core.publisher.Mono}.
     * @throws NoMoreResultException emit when {@link MultiResult} end and no buffer.
     */
    Publisher<ResultStates> nextUpdate();

}
