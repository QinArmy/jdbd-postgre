package io.jdbd.postgre.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.CursorDirection;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.VendorRefCursor;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @see <a href="https://www.postgresql.org/docs/current/sql-declare.html">define a cursor</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-fetch.html">FETCH</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-move.html">MOVE</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-close.html">CLOSE</a>
 */
final class PgRefCursor extends VendorRefCursor {

    static PgRefCursor create(String name, CursorTask task, DatabaseSession session) {
        return new PgRefCursor(name, task, session);
    }


    private final CursorTask task;

    private final DatabaseSession session;

    private final boolean autoCloseOnError;

    /**
     * private constructor
     */
    private PgRefCursor(String name, CursorTask task, DatabaseSession session) {
        super(name);
        this.task = task;
        this.session = session;
        this.autoCloseOnError = task.isAutoCloseCursorOnError();
    }

    @Override
    public DatabaseSession databaseSession() {
        return this.session;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T valueOf(final Option<T> option) {
        if (option == Option.AUTO_CLOSE_ON_ERROR) {
            return (T) Boolean.valueOf(this.autoCloseOnError);
        }
        return null;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-fetch.html">FETCH</a>
     */
    @Override
    public <T> Publisher<T> fetch(final @Nullable CursorDirection direction,
                                  final @Nullable Function<CurrentRow, T> function,
                                  final @Nullable Consumer<ResultStates> consumer) {

        final RuntimeException error;

        if (direction == null) {
            error = PgExceptions.cursorDirectionIsNull();
        } else if (function == null) {
            error = PgExceptions.queryMapFuncIsNull();
        } else if (consumer == null) {
            error = PgExceptions.statesConsumerIsNull();
        } else switch (direction) {
            case FIRST:
            case NEXT:
            case PRIOR:
            case LAST:
            case FORWARD_ALL:
            case BACKWARD_ALL:
                error = null;
                break;
            default:
                error = PgExceptions.unexpectedCursorDirection(direction);
        }

        final Flux<T> flux;
        if (error == null) {
            flux = this.task.fetch(direction, 0, function, consumer);
        } else if (this.autoCloseOnError) {
            flux = this.task.close()
                    .thenMany(Flux.error(error));
        } else {
            flux = Flux.error(error);
        }
        return flux;
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-fetch.html">FETCH</a>
     */
    @Override
    public OrderedFlux fetch(final @Nullable CursorDirection direction) {
        final RuntimeException error;
        if (direction == null) {
            error = PgExceptions.cursorDirectionIsNull();
        } else switch (direction) {
            case FIRST:
            case NEXT:
            case PRIOR:
            case LAST:
            case FORWARD_ALL:
            case BACKWARD_ALL:
                error = null;
                break;
            default:
                error = PgExceptions.unexpectedCursorDirection(direction);
        }
        final OrderedFlux flux;
        if (error == null) {
            flux = this.task.fetch(direction, 0, false);
        } else if (this.autoCloseOnError) {
            flux = MultiResults.emptyAndError(this.task.close(), error);
        } else {
            flux = MultiResults.fluxError(error);
        }
        return flux;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-fetch.html">FETCH</a>
     */
    @Override
    public <T> Publisher<T> fetch(final @Nullable CursorDirection direction, final long count,
                                  final @Nullable Function<CurrentRow, T> function,
                                  final @Nullable Consumer<ResultStates> consumer) {
        final RuntimeException error;

        if (direction == null) {
            error = PgExceptions.cursorDirectionIsNull();
        } else if (function == null) {
            error = PgExceptions.queryMapFuncIsNull();
        } else if (consumer == null) {
            error = PgExceptions.statesConsumerIsNull();
        } else switch (direction) {
            case ABSOLUTE:
            case RELATIVE:
            case FORWARD:
            case BACKWARD:
                error = null;
                break;
            default:
                error = PgExceptions.unexpectedCursorDirection(direction);
        }

        final Flux<T> flux;
        if (error == null) {
            flux = this.task.fetch(direction, count, function, consumer);
        } else if (this.autoCloseOnError) {
            flux = this.task.close()
                    .thenMany(Flux.error(error));
        } else {
            flux = Flux.error(error);
        }
        return flux;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-fetch.html">FETCH</a>
     */
    @Override
    public OrderedFlux fetch(final @Nullable CursorDirection direction, final long count) {
        final RuntimeException error;

        if (direction == null) {
            error = PgExceptions.cursorDirectionIsNull();
        } else switch (direction) {
            case ABSOLUTE:
            case RELATIVE:
            case FORWARD:
            case BACKWARD:
                error = null;
                break;
            default:
                error = PgExceptions.unexpectedCursorDirection(direction);
        }
        final OrderedFlux flux;
        if (error == null) {
            flux = this.task.fetch(direction, count, false);
        } else if (this.autoCloseOnError) {
            flux = MultiResults.emptyAndError(this.task.close(), error);
        } else {
            flux = MultiResults.fluxError(error);
        }
        return flux;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-fetch.html">FETCH</a>
     * @see <a href="https://www.postgresql.org/docs/current/sql-close.html">CLOSE</a>
     */
    @Override
    public <T> Publisher<T> forwardAllAndClosed(Function<CurrentRow, T> function) {
        return this.forwardAllAndClosed(function, Stmts.IGNORE_RESULT_STATES);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-fetch.html">FETCH</a>
     * @see <a href="https://www.postgresql.org/docs/current/sql-close.html">CLOSE</a>
     */
    @Override
    public <T> Publisher<T> forwardAllAndClosed(final @Nullable Function<CurrentRow, T> function,
                                                final @Nullable Consumer<ResultStates> consumer) {
        final RuntimeException error;
        if (function == null) {
            error = PgExceptions.queryMapFuncIsNull();
        } else if (consumer == null) {
            error = PgExceptions.statesConsumerIsNull();
        } else {
            error = null;
        }
        final Flux<T> flux;
        if (error == null) {
            flux = this.task.fetch(CursorDirection.FORWARD_ALL, 0, function, consumer)
                    .concatWith(Mono.defer(this.task::close));
        } else {
            flux = this.task.close()
                    .thenMany(Flux.error(error));
        }
        return flux;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-fetch.html">FETCH</a>
     * @see <a href="https://www.postgresql.org/docs/current/sql-close.html">CLOSE</a>
     */
    @Override
    public OrderedFlux forwardAllAndClosed() {
        return this.task.fetch(CursorDirection.FORWARD_ALL, 0, true);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-move.html">MOVE</a>
     */
    @Override
    public Publisher<ResultStates> move(final @Nullable CursorDirection direction) {
        final RuntimeException error;

        if (direction == null) {
            error = PgExceptions.cursorDirectionIsNull();
        } else switch (direction) {
            case FIRST:
            case NEXT:
            case PRIOR:
            case LAST:
            case FORWARD_ALL:
            case BACKWARD_ALL:
                error = null;
                break;
            default:
                error = PgExceptions.unexpectedCursorDirection(direction);
        }
        final Mono<ResultStates> mono;
        if (error == null) {
            mono = this.task.move(direction, 0);
        } else if (this.autoCloseOnError) {
            mono = this.task.close()
                    .then(Mono.error(error));
        } else {
            mono = Mono.error(error);
        }
        return mono;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-move.html">MOVE</a>
     */
    @Override
    public Publisher<ResultStates> move(final @Nullable CursorDirection direction, final long count) {
        final RuntimeException error;

        if (direction == null) {
            error = PgExceptions.cursorDirectionIsNull();
        } else switch (direction) {
            case ABSOLUTE:
            case RELATIVE:
            case FORWARD:
            case BACKWARD:
                error = null;
                break;
            default:
                error = PgExceptions.unexpectedCursorDirection(direction);
        }
        final Mono<ResultStates> mono;
        if (error == null) {
            mono = this.task.move(direction, count);
        } else if (this.autoCloseOnError) {
            mono = this.task.close()
                    .then(Mono.error(error));
        } else {
            mono = Mono.error(error);
        }
        return mono;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-close.html">CLOSE</a>
     */
    @Override
    public <T> Publisher<T> close() {
        return this.task.close();
    }


    @Override
    public String toString() {
        return PgStrings.builder()
                .append(getClass().getName())
                .append("[ name : ")
                .append(this.name)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }



}//PgRefCursor
