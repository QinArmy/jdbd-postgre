package io.jdbd;


import io.jdbd.lang.NonNull;

import java.sql.SQLException;

public final class JdbdSQLException extends RuntimeException {

    public JdbdSQLException(SQLException cause) {
        super(cause);
    }

    public JdbdSQLException(String message, SQLException cause) {
        super(message, cause);
    }

    public JdbdSQLException(String message, SQLException cause
            , boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    @NonNull
    @Override
    public synchronized SQLException getCause() {
        return (SQLException) super.getCause();
    }
}
