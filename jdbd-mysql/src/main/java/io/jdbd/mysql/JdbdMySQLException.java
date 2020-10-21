package io.jdbd.mysql;

import io.jdbd.JdbdRuntimeException;
import reactor.util.annotation.Nullable;

public class JdbdMySQLException extends JdbdRuntimeException {

    public JdbdMySQLException(String message, @Nullable Object... args) {
        super(createMessage(message, args));
    }

    public JdbdMySQLException(Throwable cause, String message, @Nullable Object... args) {
        super(createMessage(message, args), cause);
    }

    public JdbdMySQLException(Throwable cause, boolean enableSuppression, boolean writableStackTrace
            , String message, @Nullable Object... args) {
        super(createMessage(message, args), cause, enableSuppression, writableStackTrace);
    }


}
