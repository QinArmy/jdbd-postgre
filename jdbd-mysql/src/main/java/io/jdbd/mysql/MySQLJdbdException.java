package io.jdbd.mysql;

import io.jdbd.JdbdNonSQLException;
import reactor.util.annotation.Nullable;

public class MySQLJdbdException extends JdbdNonSQLException {

    @Deprecated
    public MySQLJdbdException(String message, Object... args) {
        super(createMessage(message, args));
    }

    @Deprecated
    public MySQLJdbdException(@Nullable Throwable cause, String message, Object... args) {
        super(createMessage(message, args), cause);
    }

    public MySQLJdbdException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }


}
