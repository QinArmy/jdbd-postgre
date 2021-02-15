package io.jdbd;


import io.jdbd.lang.Nullable;

public abstract class JdbdNonSQLException extends JdbdException {

    public JdbdNonSQLException(String message) {
        super(message);
    }

    public JdbdNonSQLException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    public JdbdNonSQLException(String message, @Nullable Throwable cause
            , boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }


}
