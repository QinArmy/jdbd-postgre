package io.jdbd.postgre.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.CursorDirection;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.session.Option;
import io.jdbd.statement.BindSingleStatement;
import io.jdbd.vendor.result.VendorRefCursor;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

abstract class PgRefCursor extends VendorRefCursor {

    static PgRefCursor cursorOfColumn(String name, PgColumnMeta meta, TaskAdjutant adjutant) {
        return new ColumnRefCursor(name, PgCursorTask.create(name, adjutant), meta);
    }

    static PgRefCursor cursorOfStatement(String name, TaskAdjutant adjutant) {
        return new StatementRefCursor(name, PgCursorTask.create(name, adjutant));
    }

    private final PgCursorTask task;

    /**
     * private constructor
     */
    private PgRefCursor(String name, PgCursorTask task) {
        super(name);
        this.task = task;
    }


    @Override
    public final <T> Publisher<T> fetch(CursorDirection direction, Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return Flux.from(fetch(direction, function))
                .onErrorResume(this::closeOnError)
                .concatWith(Mono.defer(() -> Mono.from(this.close())));

    }

    private <T> Mono<T> closeOnError(Throwable error) {
        return Mono.defer(() -> Mono.from(this.close()))
                .then(Mono.error(error));
    }


    @Override
    public final OrderedFlux fetch(CursorDirection direction) {
        return null;
    }

    @Override
    public final <T> Publisher<T> fetch(CursorDirection direction, long count, Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux fetch(CursorDirection direction, long count) {
        return null;
    }

    @Override
    public final <T> Publisher<T> forwardAllAndClosed(Function<CurrentRow, T> function) {
        return null;
    }

    @Override
    public final <T> Publisher<T> forwardAllAndClosed(Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux forwardAllAndClosed() {
        return null;
    }

    @Override
    public final Publisher<ResultStates> move(CursorDirection direction) {
        return null;
    }

    @Override
    public final Publisher<ResultStates> move(CursorDirection direction, int count) {
        return null;
    }

    @Override
    public final <T> Mono<T> close() {
        return Mono.empty();
    }


    @Override
    public final <T> T valueOf(Option<T> option) {
        return null;
    }

    private ParamStmt createFetchStmt(final int fetchSize, final Consumer<ResultStates> consumer) {
        final StringBuilder builder = new StringBuilder(40);
        final List<ParamValue> paramList = PgCollections.arrayList(2);

        builder.append("FETCH");
        if (fetchSize == 0) {
            builder.append(" ALL");
        } else {
            builder.append(" FORWARD ?");
            paramList.add(JdbdValues.paramValue(0, PgType.INTEGER, fetchSize));
        }
        builder.append(" FROM ? ");
        paramList.add(JdbdValues.paramValue(paramList.size(), PgType.TEXT, this.name));
        return Stmts.paramFetchStmt(builder.toString(), paramList, consumer, fetchSize);
    }


    private <T> Mono<T> closeCursor() {
        return Mono.empty();
    }

    private <T> Mono<T> handleFetchEnd(@Nullable Throwable error) {
        final Mono<T> mono;
        if (error == null) {
            mono = Mono.defer(this::closeCursor);
        } else if (error instanceof PgServerException) {
            // postgre protocol : current transaction is aborted, commands ignored until end of transaction
            mono = Mono.error(error);
        } else {
            mono = Mono.defer(this::closeCursor)
                    .then(Mono.error(error));
        }
        return mono;
    }


    private static final class ColumnRefCursor extends PgRefCursor {

        private final PgColumnMeta meta;

        private ColumnRefCursor(String name, PgCursorTask task, PgColumnMeta meta) {
            super(name, task);
            this.meta = meta;
        }

        @Override
        public String toString() {
            return PgStrings.builder()
                    .append(getClass().getName())
                    .append("[ name : ")
                    .append(this.name)
                    .append(" , index : ")
                    .append(this.meta.columnIndex)
                    .append(" , label : ")
                    .append(this.meta.columnLabel)
                    .append(" , hash : ")
                    .append(System.identityHashCode(this))
                    .append(" ]")
                    .toString();
        }


    }//ColumnRefCursor


    /**
     * <p>
     * This class for {@link BindSingleStatement#declareCursor()}.
     * </p>
     */
    private static final class StatementRefCursor extends PgRefCursor {

        private StatementRefCursor(String name, PgCursorTask task) {
            super(name, task);
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


    }//StatementRefCursor


}//PgRefCursor
