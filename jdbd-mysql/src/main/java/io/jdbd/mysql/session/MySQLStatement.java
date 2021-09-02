package io.jdbd.mysql.session;

import io.jdbd.DatabaseSession;
import io.jdbd.stmt.Statement;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

abstract class MySQLStatement<S extends MySQLDatabaseSession> implements Statement {

    final S session;

    MySQLStatement(S session) {
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
    public void setTimeout(int seconds) {

    }

    @Override
    public boolean setFetchSize(int fetchSize) {
        return false;
    }

    @Override
    public boolean setImportPublisher(Function<Object, Publisher<byte[]>> function) {
        return false;
    }

    @Override
    public boolean setExportSubscriber(Function<Object, Subscriber<byte[]>> function) {
        return false;
    }
}
