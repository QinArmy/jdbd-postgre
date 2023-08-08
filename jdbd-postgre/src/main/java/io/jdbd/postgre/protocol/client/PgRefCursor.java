package io.jdbd.postgre.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.statement.BindSingleStatement;
import io.jdbd.vendor.result.VendorRefCursor;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

abstract class PgRefCursor extends VendorRefCursor {

    static PgRefCursor cursorOfColumn(String name, PgColumnMeta meta, TaskAdjutant adjutant) {
        return new ColumnRefCursor(name, CursorTask.create(name, adjutant), meta);
    }

    static PgRefCursor cursorOfStatement(String name, TaskAdjutant adjutant) {
        return new StatementRefCursor(name, CursorTask.create(name, adjutant));
    }

    private final CursorTask task;

    /**
     * private constructor
     */
    private PgRefCursor(String name, CursorTask task) {
        super(name);
        this.task = task;
    }

    @Override
    public final <T> Publisher<T> first(Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux first() {
        return null;
    }

    @Override
    public final <T> Publisher<T> last(Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux last() {
        return null;
    }

    @Override
    public final <T> Publisher<T> prior(Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux prior() {
        return null;
    }

    @Override
    public final <T> Publisher<T> next(Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux next() {
        return null;
    }

    @Override
    public final <T> Publisher<T> absolute(long count, Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux absolute(long count) {
        return null;
    }

    @Override
    public final <T> Publisher<T> relative(long count, Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux relative(long count) {
        return null;
    }

    @Override
    public final <T> Publisher<T> forward(long count, Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux forward(long count) {
        return null;
    }

    @Override
    public final <T> Publisher<T> forwardAll(Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux forwardAll() {
        return null;
    }

    @Override
    public final <T> Publisher<T> allAndClose(Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux allAndClose() {
        return null;
    }

    @Override
    public final <T> Publisher<T> backward(long count, Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux backward(long count) {
        return null;
    }

    @Override
    public final <T> Publisher<T> backwardAll(Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public final OrderedFlux backwardAll() {
        return null;
    }

    @Override
    public final <T> Publisher<T> close() {
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

        private ColumnRefCursor(String name, CursorTask task, PgColumnMeta meta) {
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

        private StatementRefCursor(String name, CursorTask task) {
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
