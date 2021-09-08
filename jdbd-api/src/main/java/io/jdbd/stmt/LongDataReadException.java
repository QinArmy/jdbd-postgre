package io.jdbd.stmt;


import io.jdbd.JdbdNonSQLException;

public class LongDataReadException extends JdbdNonSQLException {

    public LongDataReadException(String messageFormat, Throwable cause) {
        super(messageFormat, cause);
    }

    public LongDataReadException(Throwable cause, String messageFormat) {
        super(cause, messageFormat);
    }

    @Deprecated
    public LongDataReadException(Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }

    @Deprecated
    public LongDataReadException(Throwable cause, boolean enableSuppression, boolean writableStackTrace
            , String messageFormat, Object... args) {
        super(cause, enableSuppression, writableStackTrace, messageFormat, args);
    }



}
