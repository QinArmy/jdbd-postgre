package io.jdbd.mysql;

import io.jdbd.DatabaseSessionFactory;
import io.jdbd.Driver;
import io.jdbd.JdbdNonSQLException;

import java.util.Map;

public final class MySQLDriver implements Driver {

    private static final MySQLDriver INSTANCE = new MySQLDriver();

    public static MySQLDriver getInstance() {
        return INSTANCE;
    }

    private MySQLDriver() {
    }

    @Override
    public boolean acceptsUrl(String url) {
        return false;
    }

    @Override
    public DatabaseSessionFactory createSessionFactory(String url, Map<String, String> properties)
            throws JdbdNonSQLException {
        return null;
    }

    @Override
    public DatabaseSessionFactory forPoolVendor(String url, Map<String, String> properties)
            throws JdbdNonSQLException {
        return null;
    }


}
