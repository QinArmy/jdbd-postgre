package io.jdbd;

import reactor.util.annotation.Nullable;

public abstract class JdbdRuntimeException extends RuntimeException {

    public JdbdRuntimeException(String message) {
        super(message);
    }

    public JdbdRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public JdbdRuntimeException(String message, Throwable cause
            , boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    protected static String createMessage(@Nullable String format, @Nullable Object... args) {
        String msg;
        if (format != null && args != null && args.length > 0) {
            msg = String.format(format, args);
        } else {
            msg = format;
        }
        return msg;
    }
}
