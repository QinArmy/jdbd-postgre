package io.jdbd.vendor;

import io.jdbd.JdbdNonSQLException;

public final class JdbdUnknownException extends JdbdNonSQLException {

    public JdbdUnknownException(Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }

    public JdbdUnknownException(Throwable cause, boolean enableSuppression, boolean writableStackTrace
            , String messageFormat, Object... args) {
        super(cause, enableSuppression, writableStackTrace, messageFormat, args);
    }


}
