package io.jdbd.vendor.task;

import io.jdbd.JdbdNonSQLException;
import reactor.util.annotation.Nullable;

public class TaskStatusException extends JdbdNonSQLException {


    public TaskStatusException(String messageFormat, Object... args) {
        super(messageFormat, args);
    }

    public TaskStatusException(@Nullable Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }

    public TaskStatusException(@Nullable Throwable cause, boolean enableSuppression
            , boolean writableStackTrace, String messageFormat, Object... args) {
        super(cause, enableSuppression, writableStackTrace, messageFormat, args);
    }


}
