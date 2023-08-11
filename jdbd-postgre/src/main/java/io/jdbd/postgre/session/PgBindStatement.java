package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.*;
import io.jdbd.statement.BindStatement;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * <p>
 * This class is a implementation of {@link BindStatement} with postgre client protocol.
 * </p>
 *
 * @see PgDatabaseSession#bindStatement(String)
 */
final class PgBindStatement extends PgStatement<BindStatement> implements BindStatement {

    /**
     * @see PgDatabaseSession#bindStatement(String)
     */
    static PgBindStatement create(String sql, PgDatabaseSession<?> session, boolean forceServerPrepared) {
        if (!PgStrings.hasText(sql)) {
            throw new IllegalArgumentException("sql must be have text.");
        }
        return new PgBindStatement(sql, session);
    }

    private final String sql;

    private final List<List<BindValue>> paramGroupList = new LinkedList<>();

    private List<BindValue> paramGroup = new ArrayList<>();

    private int firstGroupSize = -1;

    private int fetchSize = 0;


    private PgBindStatement(String sql, PgDatabaseSession session) {
        super(session);
        this.sql = sql;
    }

    @Override
    public boolean isForcePrepare() {
        return false;
    }


    @Override
    public BindStatement bind(int indexBasedZero, DataType dataType, @Nullable Object value) throws JdbdException {
        return null;
    }

    @Override
    public BindStatement addBatch() throws JdbdException {
        return null;
    }


    @Override
    public Publisher<ResultStates> executeUpdate() {
        return null;
    }

    @Override
    public Publisher<ResultRow> executeQuery() {
        return null;
    }

    @Override
    public <R> Publisher<R> executeQuery(Function<CurrentRow, R> function) {
        return null;
    }

    @Override
    public <R> Publisher<R> executeQuery(Function<CurrentRow, R> function, Consumer<ResultStates> statesConsumer) {
        return null;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-declare.html">define a cursor</a>
     */
    @Override
    public Publisher<RefCursor> declareCursor() {
        return null;
    }


    @Override
    public Publisher<ResultStates> executeBatchUpdate() {
        return null;
    }

    @Override
    public BatchQuery executeBatchQuery() {
        return null;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        return null;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {
        return null;
    }
    /*################################## blow private method ##################################*/

    /**
     * @throws JdbdException when indexBasedZero error
     */
    private int checkIndex(final int indexBasedZero) throws JdbdException {
        if (indexBasedZero < 0) {
            throw PgExceptions.invalidParameterValue(this.paramGroupList.size(), indexBasedZero);
        }
        final int firstGroupSize = this.firstGroupSize;
        if (firstGroupSize > -1 && indexBasedZero >= firstGroupSize) {
            throw PgExceptions.beyondFirstParamGroupRange(indexBasedZero, firstGroupSize);
        }
        return indexBasedZero;
    }


}
