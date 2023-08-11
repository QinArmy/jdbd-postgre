package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.task.PrepareTask;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * <p>
 * This class is a implementation of {@link PreparedStatement} with postgre client protocol.
 * </p>
 */
final class PgPreparedStatement extends PgStatement<PreparedStatement> implements PreparedStatement {

    static PgPreparedStatement create(PgDatabaseSession<?> session, PrepareTask stmtTask) {
        return new PgPreparedStatement(session, stmtTask);
    }


    private final PrepareTask stmtTask;

    private final String sql;

    private final List<? extends DataType> paramTypeList;

    private final int paramCount;

    private final ResultRowMeta rowMeta;

    private final Warning warning;

    private List<List<ParamValue>> paramGroupList;

    private List<ParamValue> paramGroup;

    private PgPreparedStatement(PgDatabaseSession<?> session, PrepareTask stmtTask) {
        super(session);

        this.stmtTask = stmtTask;
        this.sql = stmtTask.getSql();
        this.paramTypeList = stmtTask.getParamTypes();
        this.paramCount = this.paramTypeList.size();

        this.rowMeta = stmtTask.getRowMeta();
        this.warning = stmtTask.getWarning();
    }


    @Override
    public List<? extends DataType> paramTypeList() {
        return this.paramTypeList;
    }

    @Override
    public ResultRowMeta resultRowMeta() {
        return this.rowMeta;
    }

    @Override
    public Warning waring() {
        return this.warning;
    }

    @Override
    public PreparedStatement bind(final int indexBasedZero, final @Nullable DataType dataType, final @Nullable Object value)
            throws JdbdException {

        List<ParamValue> paramGroup = this.paramGroup;

        if (paramGroup == EMPTY_PARAM_GROUP) {
            throw PgExceptions.cannotReuseStatement(PreparedStatement.class);
        }

        final int paramCount = this.paramCount;
        final RuntimeException error;
        final DataType type;

        if (indexBasedZero < 0 || indexBasedZero == paramCount) {
            List<List<ParamValue>> paramGroupList = this.paramGroupList;
            final int groupSize = paramGroupList == null ? 0 : paramGroupList.size();
            error = PgExceptions.invalidParameterValue(groupSize, indexBasedZero);
        } else if (dataType == null) {
            error = PgExceptions.dataTypeIsNull();
        } else if (value != null && dataType == JdbdType.NULL) {
            error = PgExceptions.nonNullBindValueOf(dataType);
        } else if ((type = PgBinds.mapDataType(dataType)) == null) {
            error = PgExceptions.dontSupportDataType(dataType, PgDriver.POSTGRE_SQL);
        } else {
            error = null;
            if (paramGroup == null) {
                this.paramGroup = paramGroup = PgCollections.arrayList(paramCount);
            }
            paramGroup.add(JdbdValues.paramValue(indexBasedZero, type, value));
        }

        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            clearStatementToAvoidReuse();
            throw error;
        }
        return this;
    }

    @Override
    public PreparedStatement addBatch() throws JdbdException {
        return null;
    }

    @Override
    public Publisher<ResultStates> executeUpdate() {
        return null;
    }

    @Override
    public Publisher<ResultRow> executeQuery() {
        return this.executeQuery(DatabaseProtocol.ROW_FUNC, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(Function<CurrentRow, R> function) {
        return this.executeQuery(function, Stmts.IGNORE_RESULT_STATES);
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

    @Override
    public DatabaseSession abandonBind() {
        this.clearStatementToAvoidReuse();
        this.stmtTask.abandonBind();
        return this.session;
    }


    @Override
    void closeOnBindError(Throwable error) {
        this.stmtTask.closeOnBindError(error);
        this.clearStatementToAvoidReuse();
    }


    /*################################## blow private method ##################################*/

    private void clearStatementToAvoidReuse() {
        this.paramGroupList = null;
        this.paramGroup = EMPTY_PARAM_GROUP;
    }

}
