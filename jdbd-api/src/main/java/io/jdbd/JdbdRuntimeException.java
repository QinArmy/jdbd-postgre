package io.jdbd;

public abstract class JdbdRuntimeException extends RuntimeException {

    public JdbdRuntimeException(String message) {
        super(message);
    }

    public JdbdRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public JdbdRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
