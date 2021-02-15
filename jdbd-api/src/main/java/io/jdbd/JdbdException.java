package io.jdbd;

import io.jdbd.lang.Nullable;

public class JdbdException extends RuntimeException {

    public JdbdException(String message) {
        super(message);
    }

    public JdbdException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    public JdbdException(Throwable cause) {
        super(cause);
    }

    public JdbdException(String message, @Nullable Throwable cause
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
