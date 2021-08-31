package io.jdbd.postgre.session;

import io.jdbd.DatabaseSession;
import io.jdbd.stmt.PreparedStatement;
import org.reactivestreams.Publisher;

public abstract class PgDatabaseSession implements DatabaseSession {


    @Override
    public final Publisher<PreparedStatement> prepare(String sql) {
        return null;
    }


}
