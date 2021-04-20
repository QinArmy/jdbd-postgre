package io.jdbd.mysql.session;

import io.jdbd.DatabaseSession;
import io.jdbd.stmt.Statement;

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


}
