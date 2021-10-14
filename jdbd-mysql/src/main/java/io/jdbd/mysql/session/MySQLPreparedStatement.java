package io.jdbd.mysql.session;

import io.jdbd.DatabaseSession;
import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.AttrPreparedStatement;
import io.jdbd.result.*;
import io.jdbd.vendor.task.PrepareStmtTask;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.function.Consumer;

public final class MySQLPreparedStatement extends MySQLStatement implements AttrPreparedStatement {

    public static MySQLPreparedStatement create(MySQLDatabaseSession session, PrepareStmtTask<MySQLType> task) {
        return new MySQLPreparedStatement(session, task);
    }

    private final PrepareStmtTask<MySQLType> task;

    private final List<MySQLType> paramTypes;

    private final ResultRowMeta resultRowMeta;

    private MySQLPreparedStatement(final MySQLDatabaseSession session, final PrepareStmtTask<MySQLType> task) {
        super(session);
        this.task = task;
        this.paramTypes = task.getParamTypes();
        this.resultRowMeta = task.getRowMeta();
    }


    @Override
    public void bind(final int indexBasedZero, final @Nullable Object nullable) {

    }

    @Override
    public void bindCommonAttr(final String name, final MySQLType type, final @Nullable Object value) {

    }

    @Override
    public void bindAttr(final String name, final MySQLType type, final @Nullable Object value) {

    }


    @Override
    public void addBatch() {

    }

    @Override
    public List<? extends SQLType> getParameterTypes() {
        return this.paramTypes;
    }

    @Nullable
    @Override
    public ResultRowMeta getResultRowMeta() throws JdbdSQLException {
        return this.resultRowMeta;
    }

    @Override
    public boolean setFetchSize(int fetchSize) {
        return false;
    }

    @Override
    public boolean supportPublisher() {
        return false;
    }

    @Override
    public boolean supportOutParameter() {
        return false;
    }

    @Override
    public Flux<ResultStates> executeBatch() {
        return null;
    }

    @Override
    public Mono<ResultStates> executeUpdate() {
        return null;
    }

    @Override
    public Flux<ResultRow> executeQuery() {
        return null;
    }

    @Override
    public Flux<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer) {
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
    public Publisher<DatabaseSession> abandonBind() {
        return null;
    }


}
