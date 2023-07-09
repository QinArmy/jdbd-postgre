package io.jdbd.vendor.stmt;

import io.jdbd.JdbdException;

public final class CannotReuseStatementException extends JdbdException {

    public CannotReuseStatementException(String message) {
        super(message);
    }


}
