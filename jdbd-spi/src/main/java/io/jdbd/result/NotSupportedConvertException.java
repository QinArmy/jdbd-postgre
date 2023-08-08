package io.jdbd.result;


import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;

@Deprecated
public class NotSupportedConvertException extends JdbdException {

    public NotSupportedConvertException(String message) {
        super(message);
    }

    public NotSupportedConvertException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

}
