package io.jdbd.postgre;

import io.jdbd.JdbdException;

public class PgJdbdException extends JdbdException {

    public PgJdbdException(String message) {
        super(message);
    }

    public PgJdbdException(String message, Throwable cause) {
        super(message, cause);
    }

    public PgJdbdException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }


}
