package io.jdbd;

public final class ResultStateConsumerException extends JdbdNonSQLException {

    @Deprecated
    public ResultStateConsumerException(Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }

    public ResultStateConsumerException(String message, Throwable cause) {
        super(message, cause);
    }

}
