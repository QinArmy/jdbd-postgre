package io.jdbd.mysql;

import io.jdbd.JdbdNonSQLException;
import reactor.util.annotation.Nullable;

public class MySQLJdbdException extends JdbdNonSQLException {

    public MySQLJdbdException(String message, Object... args) {
        super(createMessage(message, args));
    }

    public MySQLJdbdException(@Nullable Throwable cause, String message, Object... args) {
        super(createMessage(message, args), cause);
    }



}
