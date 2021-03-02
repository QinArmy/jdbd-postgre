package io.jdbd;

public class SessionCloseException extends JdbdNonSQLException {

    public SessionCloseException(String messageFormat, Object... args) {
        super(messageFormat, args);
    }

    public SessionCloseException(Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }

    public SessionCloseException(Throwable cause, boolean enableSuppression, boolean writableStackTrace
            , String messageFormat, Object... args) {
        super(cause, enableSuppression, writableStackTrace, messageFormat, args);
    }

}
