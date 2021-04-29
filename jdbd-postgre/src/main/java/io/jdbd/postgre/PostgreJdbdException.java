package io.jdbd.postgre;

import io.jdbd.JdbdNonSQLException;

public class PostgreJdbdException extends JdbdNonSQLException {

    public PostgreJdbdException(String message) {
        super(message);
    }

    public PostgreJdbdException(String message, Throwable cause) {
        super(message, cause);
    }

    public PostgreJdbdException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }


}
