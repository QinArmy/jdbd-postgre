package io.jdbd.mysql;

import io.jdbd.JdbdException;
import reactor.util.annotation.Nullable;

public class MySQLJdbdException extends JdbdException {

    @Deprecated
    public MySQLJdbdException(String message, Object... args) {
        super(createMessage(message, args));
    }

    public MySQLJdbdException(String message) {
        super(message);
    }


    @Deprecated
    public MySQLJdbdException(@Nullable Throwable cause, String message, Object... args) {
        super(createMessage(message, args), cause);
    }

    public MySQLJdbdException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }


}
