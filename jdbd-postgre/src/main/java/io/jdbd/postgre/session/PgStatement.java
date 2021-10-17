package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgType;
import io.jdbd.session.DatabaseSession;
import io.jdbd.stmt.Statement;
import io.jdbd.vendor.stmt.StatementOption;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.util.Objects;
import java.util.function.Function;

/**
 * <p>
 * This class is base class of below:
 *     <ul>
 *         <li>{@link PgPreparedStatement}</li>
 *     </ul>
 * </p>
 */
abstract class PgStatement implements Statement, StatementOption {

    final PgDatabaseSession session;

    private int timeout = 0;

    private Function<Object, Publisher<byte[]>> importPublisher;

    private Function<Object, Subscriber<byte[]>> exportPublisher;


    PgStatement(PgDatabaseSession session) {
        this.session = session;
    }


    @Override
    public final DatabaseSession getSession() {
        return this.session;
    }

    @Override
    public final <T extends DatabaseSession> T getSession(Class<T> sessionClass) {
        return sessionClass.cast(this.session);
    }

    @Override
    public final void setTimeout(int seconds) {
        this.timeout = seconds;
    }


    @Override
    public boolean setFetchSize(int fetchSize) {
        return false;
    }

    @Override
    public final boolean setImportPublisher(Function<Object, Publisher<byte[]>> function) {
        this.importPublisher = function;
        return true;
    }

    @Override
    public final boolean setExportSubscriber(Function<Object, Subscriber<byte[]>> function) {
        this.exportPublisher = function;
        return true;
    }

    @Override
    public boolean supportPublisher() {
        return false;
    }

    @Override
    public final boolean supportOutParameter() {
        return true;
    }



    /*################################## blow StatementOption method ##################################*/


    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public final int getTimeout() {
        return this.timeout;
    }


    @Nullable
    @Override
    public final Function<Object, Publisher<byte[]>> getImportPublisher() {
        final Function<Object, Publisher<byte[]>> function = this.importPublisher;
        if (function != null) {
            this.importPublisher = null;
        }
        return function;
    }

    @Nullable
    @Override
    public final Function<Object, Subscriber<byte[]>> getExportSubscriber() {
        final Function<Object, Subscriber<byte[]>> function = this.exportPublisher;
        if (function != null) {
            this.exportPublisher = null;
        }
        return function;
    }


    /*################################## blow packet static method ##################################*/


    static PgType checkSqlType(final SQLType sqlType) throws JdbdException {
        Objects.requireNonNull(sqlType, "sqlType");
        if (!(sqlType instanceof PgType)) {
            String m = String.format("sqlType isn't a instance of %s", PgType.class.getName());
            throw new PgJdbdException(m);
        }
        return (PgType) sqlType;

    }

}
