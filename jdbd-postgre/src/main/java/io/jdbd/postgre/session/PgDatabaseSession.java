package io.jdbd.postgre.session;

import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.protocol.client.ClientProtocol;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import io.jdbd.statement.*;
import io.jdbd.vendor.task.PrepareTask;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * This class is a implementation of {@link DatabaseSession} with postgre client protocol.
 * <p>
 * This class is base class of :
 *     <ul>
 *         <li>{@link PgLocalDatabaseSession}</li>
 *         <li>{@link PgXaDatabaseSession}</li>
 *     </ul>
 * </p>
 */
abstract class PgDatabaseSession implements DatabaseSession {

    final SessionAdjutant adjutant;

    final ClientProtocol protocol;

    PgDatabaseSession(SessionAdjutant adjutant, ClientProtocol protocol) {
        this.adjutant = adjutant;
        this.protocol = protocol;
    }


    @Override
    public final Publisher<ResultStates> executeUpdate(String sql) {
        return null;
    }

    @Override
    public final <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function) {
        return null;
    }

    @Override
    public final <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function, Consumer<ResultStates> statesConsumer) {
        return null;
    }

    @Override
    public final Publisher<ResultStates> executeBatchUpdate(List<String> sqlGroup) {
        return null;
    }

    @Override
    public final MultiResult executeBatchAsMulti(List<String> sqlGroup) {
        return null;
    }

    @Override
    public final OrderedFlux executeBatchAsFlux(List<String> sqlGroup) {
        return null;
    }

    @Override
    public final OrderedFlux executeAsFlux(String multiStmt) {
        return null;
    }

    @Override
    public final Mono<TransactionOption> getTransactionOption() {
        return null;
    }

    @Override
    public final DatabaseMetaData getDatabaseMetaData() {
        return null;
    }


    @Override
    public final StaticStatement statement() {
        return PgStaticStatement.create(this);
    }

    @Override
    public final Mono<PreparedStatement> prepare(final String sql) {
        return this.protocol.prepare(sql, this::createPreparedStatement);
    }

    @Override
    public OneStepPrepareStatement oneStep(String sql) {
        return null;
    }

    @Override
    public final BindStatement bindStatement(final String sql) {
        return PgBindStatement.create(sql, this);
    }

    @Override
    public final MultiStatement multiStatement() {
        return PgMultiStatement.create(this);
    }

    @Override
    public final boolean supportSavePoints() {
        return true;
    }

    @Override
    public boolean supportMultiStatement() {
        return true;
    }


    @Override
    public final Publisher<SavePoint> setSavePoint() {
        return null;
    }

    @Override
    public final Publisher<SavePoint> setSavePoint(String name) {
        return null;
    }

    @Override
    public final Publisher<Void> releaseSavePoint(SavePoint savepoint) {
        return null;
    }

    @Override
    public final Publisher<Void> rollbackToSavePoint(SavePoint savepoint) {
        return null;
    }

    @Override
    public final boolean isClosed() {
        return this.protocol.isClosed();
    }

    @Override
    public final ServerVersion getServerVersion() {
        return this.protocol.getServerVersion();
    }

    @Override
    public final Mono<Void> close() {
        return this.protocol.close();
    }

    @Override
    public final boolean isSameFactory(DatabaseSession session) {
        return session instanceof PgDatabaseSession
                && ((PgDatabaseSession) session).adjutant == this.adjutant;
    }

    @Override
    public final boolean isBelongTo(DatabaseSessionFactory factory) {
        return this.adjutant.isSameFactory(factory);
    }

    /*################################## blow private method ##################################*/

    private PgPreparedStatement createPreparedStatement(final PrepareTask<PgType> stmtTask) {
        return PgPreparedStatement.create(this, stmtTask);
    }


}
