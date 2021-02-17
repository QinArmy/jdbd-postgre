package io.jdbd;


import reactor.util.annotation.Nullable;

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

    public JdbdNonSQLException(String messageFormat, Object... args) {
        super(createMessage(messageFormat, args));
    }

    public JdbdNonSQLException(@Nullable Throwable cause, String messageFormat, Object... args) {
        super(createMessage(messageFormat, args), cause);

    }

    public JdbdNonSQLException(@Nullable Throwable cause, boolean enableSuppression
            , boolean writableStackTrace, String messageFormat, Object... args) {
        super(createMessage(messageFormat, args), cause, enableSuppression, writableStackTrace);
    }


}
