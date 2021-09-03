package io.jdbd.vendor.stmt;

import io.jdbd.JdbdNonSQLException;

public final class CannotReuseStatementException extends JdbdNonSQLException {

    public CannotReuseStatementException(String message) {
        super(message);
    }


}
