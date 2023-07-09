package io.jdbd.statement;

import io.jdbd.JdbdException;


public final class TaskQueueOverflowException extends JdbdException {

    public TaskQueueOverflowException(String message) {
        super(message);
    }


    public TaskQueueOverflowException(String message, Throwable cause) {
        super(message, cause);
    }


}
