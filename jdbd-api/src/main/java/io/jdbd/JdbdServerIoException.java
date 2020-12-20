package io.jdbd;

public class JdbdServerIoException extends JdbdIoException {

    public JdbdServerIoException(String message) {
        super(message);
    }

    public JdbdServerIoException(String message, Throwable cause) {
        super(message, cause);
    }

    public JdbdServerIoException(String message, Throwable cause
            , boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
