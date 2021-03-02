package io.jdbd;

public class StatementCloseException extends JdbdNonSQLException {

    public StatementCloseException(String messageFormat, Object... args) {
        super(messageFormat, args);
    }

    public StatementCloseException(Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }

    public StatementCloseException(Throwable cause, boolean enableSuppression, boolean writableStackTrace
            , String messageFormat, Object... args) {
        super(cause, enableSuppression, writableStackTrace, messageFormat, args);
    }
}
