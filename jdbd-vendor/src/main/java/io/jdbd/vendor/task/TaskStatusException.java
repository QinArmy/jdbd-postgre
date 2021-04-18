package io.jdbd.vendor.task;

import io.jdbd.JdbdNonSQLException;
import reactor.util.annotation.Nullable;

final class TaskStatusException extends JdbdNonSQLException {


    TaskStatusException(String messageFormat) {
        super(messageFormat);
    }

    TaskStatusException(@Nullable Throwable cause, String messageFormat) {
        super(cause, messageFormat);
    }


}
