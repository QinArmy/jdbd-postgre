package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.session.ChunkOption;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.statement.BindSingleStatement;
import io.jdbd.statement.Statement;
import io.jdbd.vendor.stmt.NamedValue;
import io.jdbd.vendor.stmt.StmtOption;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * <p>
 * This class is base class of following :
 *     <ul>
 *         <li>{@link PgStaticStatement}</li>
 *         <li>{@link PgPreparedStatement}</li>
 *         <li>{@link PgBindStatement}</li>
 *         <li>{@link PgMultiStatement}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
abstract class PgStatement<S extends Statement> implements Statement, StmtOption {


    final PgDatabaseSession<?> session;

    private int timeout = 0;

    int fetchSize = 0;

    private Function<ChunkOption, Publisher<byte[]>> importPublisher;

    private Function<ChunkOption, Subscriber<byte[]>> exportPublisher;


    PgStatement(PgDatabaseSession<?> session) {
        this.session = session;
    }

    @Override
    public final S bindStmtVar(String name, DataType dataType, @Nullable Object value) throws JdbdException {
        final JdbdException error;
        error = PgExceptions.dontSupportStmtVar(PgDriver.POSTGRE_SQL);
        closeOnBindError(error);
        throw error;
    }


    @Override
    public final boolean isSupportStmtVar() {
        // always false, postgre don't support statement variable.
        return false;
    }


    @Override
    public final boolean isSupportPublisher() {
        //  server-prepare statement support
        return this instanceof BindSingleStatement;
    }

    @Override
    public final boolean isSupportPath() {
        // server-prepare statement support
        return this instanceof BindSingleStatement;
    }

    @Override
    public final boolean isSupportOutParameter() {
        // server-prepare statement support
        return this instanceof BindSingleStatement;
    }

    @Override
    public final <T> T valueOf(Option<T> option) {
        //TODO
        return null;
    }

    @Override
    public final List<NamedValue> getStmtVarList() {
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    @Override
    public final S setTimeout(int seconds) {
        if (seconds < 0) {
            final IllegalArgumentException error;
            error = PgExceptions.timeoutIsNegative(seconds);
            closeOnBindError(error);
            throw error;
        }
        this.timeout = seconds;
        return (S) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final S setFetchSize(final int fetchSize) {
        if (fetchSize < 0) {
            final IllegalArgumentException error;
            error = PgExceptions.fetchSizeIsNegative(fetchSize);
            closeOnBindError(error);
            throw error;
        }
        this.fetchSize = fetchSize;
        return (S) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final S setImportPublisher(Function<ChunkOption, Publisher<byte[]>> function) throws JdbdException {
        this.importPublisher = function;
        return (S) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final S setExportSubscriber(Function<ChunkOption, Subscriber<byte[]>> function) throws JdbdException {
        this.exportPublisher = function;
        return (S) this;
    }

    @Override
    public final <T> S setOption(Option<T> option, @Nullable T value) throws JdbdException {
        final JdbdException error;
        error = PgExceptions.dontSupportSetOption(option);
        closeOnBindError(error);
        throw error;
    }

    @Override
    public final List<Option<?>> supportedOptionList() {
        return Collections.emptyList();
    }

    @Override
    public final DatabaseSession getSession() {
        return this.session;
    }

    @Override
    public final <T extends DatabaseSession> T getSession(Class<T> sessionClass) {
        try {
            return sessionClass.cast(this.session);
        } catch (ClassCastException e) {
            closeOnBindError(e);
            throw e;
        }
    }


    /*################################## blow StatementOption method ##################################*/


    @Override
    public final int getFetchSize() {
        return this.fetchSize;
    }

    @Override
    public final int getTimeout() {
        return this.timeout;
    }


    @Nullable
    @Override
    public final Function<ChunkOption, Publisher<byte[]>> getImportFunction() {
        return this.importPublisher;
    }

    @Nullable
    @Override
    public final Function<ChunkOption, Subscriber<byte[]>> getExportFunction() {
        return this.exportPublisher;
    }

    @Override
    public final DatabaseSession databaseSession() {
        return this.session;
    }

    /**
     * just for {@link PgMultiStatement}
     */
    final void resetChunkOptions() {
        this.importPublisher = null;
        this.exportPublisher = null;
    }


    /**
     * @see PgPreparedStatement
     */
    void closeOnBindError(Throwable error) {
        // no-op
    }

    /*################################## blow packet static method ##################################*/


    static PgType checkSqlType(final DataType sqlType) throws JdbdException {
        Objects.requireNonNull(sqlType, "sqlType");
        if (!(sqlType instanceof PgType)) {
            String m = String.format("sqlType isn't a instance of %s", PgType.class.getName());
            throw new JdbdException(m);
        }
        return (PgType) sqlType;

    }

}
