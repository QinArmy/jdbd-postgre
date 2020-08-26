package io.jdbd;

import io.jdbd.lang.NonNull;

import java.sql.SQLException;

public final class ReactiveSQLException extends RuntimeException {

    public ReactiveSQLException(SQLException cause) {
        super(cause);
    }

    public ReactiveSQLException(SQLException cause, String message) {
        super(message, cause);
    }

    @NonNull
    @Override
    public synchronized SQLException getCause() {
        return (SQLException) super.getCause();
    }
}
