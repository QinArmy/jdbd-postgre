package io.jdbd;


import io.jdbd.lang.Nullable;

public abstract class JdbdNonSQLException extends JdbdException {

    public JdbdNonSQLException(String message) {
        super(message);
    }

    @Deprecated
    public JdbdNonSQLException(@Nullable Throwable cause, String message) {
        super(message, cause);
    }


    public JdbdNonSQLException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    public JdbdNonSQLException(String message, @Nullable Throwable cause
            , boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    @Deprecated
    public JdbdNonSQLException(String messageFormat, Object... args) {
        super(createMessage(messageFormat, args));
    }

    @Deprecated
    public JdbdNonSQLException(@Nullable Throwable cause, String messageFormat, Object... args) {
        super(createMessage(messageFormat, args), cause);

    }

    @Deprecated
    public JdbdNonSQLException(@Nullable Throwable cause, boolean enableSuppression
            , boolean writableStackTrace, String messageFormat, Object... args) {
        super(createMessage(messageFormat, args), cause, enableSuppression, writableStackTrace);
    }


}
