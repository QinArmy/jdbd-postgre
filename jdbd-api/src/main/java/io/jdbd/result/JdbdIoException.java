package io.jdbd.result;

import io.jdbd.JdbdNonSQLException;

public class JdbdIoException extends JdbdNonSQLException {

    public JdbdIoException(String message) {
        super(message);
    }

    public JdbdIoException(String message, Throwable cause) {
        super(message, cause);
    }

    public JdbdIoException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
