package io.jdbd.vendor.stmt;

import io.jdbd.JdbdNonSQLException;

public final class CannotReuseMultiStmtException extends JdbdNonSQLException {

    public CannotReuseMultiStmtException(String message) {
        super(message);
    }


}
