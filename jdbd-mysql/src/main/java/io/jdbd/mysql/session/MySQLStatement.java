package io.jdbd.mysql.session;

import io.jdbd.DatabaseSession;
import io.jdbd.stmt.Statement;
import io.jdbd.vendor.stmt.StatementOption;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

abstract class MySQLStatement implements Statement {

    final MySQLDatabaseSession session;

    final MySQLStatementOption statementOption = new MySQLStatementOption();

    MySQLStatement(MySQLDatabaseSession session) {
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
    public boolean supportPublisher() {
        return false;
    }

    @Override
    public boolean supportOutParameter() {
        return false;
    }

    @Override
    public final void setTimeout(final int seconds) {
        this.statementOption.timeoutSeconds = seconds;
    }

    @Override
    public boolean setFetchSize(int fetchSize) {
        return false;
    }

    @Override
    public final boolean setImportPublisher(Function<Object, Publisher<byte[]>> function) {
        return false;
    }

    @Override
    public final boolean setExportSubscriber(Function<Object, Subscriber<byte[]>> function) {
        return false;
    }


    static final class MySQLStatementOption implements StatementOption {

        private int timeoutSeconds;

        int fetchSize;

        @Override
        public int getTimeout() {
            return this.timeoutSeconds;
        }

        @Override
        public int getFetchSize() {
            final int fetchSize = this.fetchSize;
            if (fetchSize > 0) {
                this.fetchSize = 0;
            }
            return fetchSize;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }

    }


}
