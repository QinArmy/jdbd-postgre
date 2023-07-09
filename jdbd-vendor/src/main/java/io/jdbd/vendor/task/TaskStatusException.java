package io.jdbd.vendor.task;

import io.jdbd.JdbdException;
import reactor.util.annotation.Nullable;

final class TaskStatusException extends JdbdException {


    TaskStatusException(String messageFormat) {
        super(messageFormat);
    }

    TaskStatusException(@Nullable Throwable cause, String messageFormat) {
        super(cause, messageFormat);
    }


}
