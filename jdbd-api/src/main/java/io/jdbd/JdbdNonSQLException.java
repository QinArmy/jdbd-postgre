package io.jdbd;


import io.jdbd.lang.Nullable;

public abstract class JdbdNonSQLException extends RuntimeException {

    public JdbdNonSQLException(String message) {
        super(message);
    }

    public JdbdNonSQLException(String message, Throwable cause) {
        super(message, cause);
    }

    public JdbdNonSQLException(String message, Throwable cause
            , boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    protected static String createMessage(@Nullable String format, @Nullable Object... args) {
        String msg;
        if (format != null && args != null && args.length > 0) {
            msg = String.format(format, args);
        } else {
            msg = format;
        }
        return msg;
    }
}
