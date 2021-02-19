package io.jdbd;

public final class ResultStateConsumerException extends JdbdNonSQLException {

    public ResultStateConsumerException(Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }

    public ResultStateConsumerException(Throwable cause, boolean enableSuppression
            , boolean writableStackTrace, String messageFormat, Object... args) {
        super(cause, enableSuppression, writableStackTrace, messageFormat, args);
    }

}
