package io.jdbd.result;

import io.jdbd.JdbdException;

public class JdbdIoException extends JdbdException {

    public JdbdIoException(String message) {
        super(message);
    }

    public JdbdIoException(String message, Throwable cause) {
        super(message, cause);
    }

    public JdbdIoException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
