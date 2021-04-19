package io.jdbd.vendor;

import io.jdbd.JdbdNonSQLException;

public final class JdbdUnknownException extends JdbdNonSQLException {

    public JdbdUnknownException(String message, Throwable cause) {
        super(cause, message);
    }

}
