package io.jdbd;


import io.jdbd.lang.Nullable;

public class NotSupportedConvertException extends JdbdNonSQLException {

    public NotSupportedConvertException(String message) {
        super(message);
    }

    public NotSupportedConvertException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

}
