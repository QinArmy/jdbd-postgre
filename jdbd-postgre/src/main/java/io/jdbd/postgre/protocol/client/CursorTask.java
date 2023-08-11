package io.jdbd.postgre.protocol.client;

import io.jdbd.result.CurrentRow;
import io.jdbd.result.CursorDirection;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.session.Closeable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This interface representing cursor task for higher performance.
 * </p>
 *
 * @see io.jdbd.result.RefCursor
 * @see PgRefCursor
 * @see <a href="https://www.postgresql.org/docs/current/sql-move.html">MOVE cursor</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-fetch.html">FETCH by cursor</a>
 * @since 1.0
 */
interface CursorTask extends Closeable {

    <T> Flux<T> fetch(CursorDirection direction, long count, Function<CurrentRow, T> function, Consumer<ResultStates> consumer);

    OrderedFlux fetch(CursorDirection direction, long count, boolean close);


    Mono<ResultStates> move(CursorDirection direction, long count);


    boolean isAutoCloseCursorOnError();

    @Override
    <T> Mono<T> close();
}
