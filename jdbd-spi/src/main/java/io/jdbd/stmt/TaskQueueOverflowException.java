package io.jdbd.stmt;

import io.jdbd.JdbdNonSQLException;

public class TaskQueueOverflowException extends JdbdNonSQLException {

    public TaskQueueOverflowException(String message) {
        super(message);
    }

    public TaskQueueOverflowException(String message, Throwable cause) {
        super(message, cause);
    }

    public TaskQueueOverflowException(String message, Throwable cause
            , boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }


}
