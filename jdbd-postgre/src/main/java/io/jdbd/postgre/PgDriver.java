package io.jdbd.postgre;

import io.jdbd.DatabaseSessionFactory;
import io.jdbd.Driver;
import io.jdbd.JdbdNonSQLException;
import io.jdbd.postgre.session.PgDatabaseSessionFactory;

import java.util.Map;

public final class PgDriver implements Driver {

    private static final PgDriver INSTANCE = new PgDriver();

    public static PgDriver getInstance() {
        return INSTANCE;
    }


    private PgDriver() {
    }


    @Override
    public final boolean acceptsUrl(String url) {
        return PgDatabaseSessionFactory.acceptsUrl(url);
    }

    @Override
    public final DatabaseSessionFactory createSessionFactory(String url, Map<String, String> properties)
            throws JdbdNonSQLException {
        return PgDatabaseSessionFactory.create(url, properties);
    }

    @Override
    public final DatabaseSessionFactory forPoolVendor(String url, Map<String, String> properties)
            throws JdbdNonSQLException {
        return PgDatabaseSessionFactory.forPoolVendor(url, properties);
    }


}
