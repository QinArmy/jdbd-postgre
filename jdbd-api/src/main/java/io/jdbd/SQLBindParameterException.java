package io.jdbd;

import reactor.util.annotation.Nullable;

@Deprecated
public class SQLBindParameterException extends JdbdNonSQLException {

    public SQLBindParameterException(String message) {
        super(message);
    }

    public SQLBindParameterException(String message, Throwable cause) {
        super(message, cause);
    }

    public SQLBindParameterException(String messageFormat, Object... args) {
        super(messageFormat, args);
    }

    public SQLBindParameterException(@Nullable Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }

    public SQLBindParameterException(@Nullable Throwable cause, boolean enableSuppression
            , boolean writableStackTrace, String messageFormat, Object... args) {
        super(cause, enableSuppression, writableStackTrace, messageFormat, args);
    }


}
