package io.jdbd.mysql;

import io.jdbd.JdbdNonSQLException;
import reactor.util.annotation.Nullable;

public class MySQLJdbdException extends JdbdNonSQLException {

    public MySQLJdbdException(String message, @Nullable Object... args) {
        super(createMessage(message, args));
    }

    public MySQLJdbdException(Throwable cause, String message, @Nullable Object... args) {
        super(createMessage(message, args), cause);
    }

    public MySQLJdbdException(Throwable cause, boolean enableSuppression, boolean writableStackTrace
            , String message, @Nullable Object... args) {
        super(createMessage(message, args), cause, enableSuppression, writableStackTrace);
    }


}
