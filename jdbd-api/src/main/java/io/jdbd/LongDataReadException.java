package io.jdbd;


public class LongDataReadException extends JdbdNonSQLException {

    public LongDataReadException(Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }

    public LongDataReadException(Throwable cause, boolean enableSuppression, boolean writableStackTrace
            , String messageFormat, Object... args) {
        super(cause, enableSuppression, writableStackTrace, messageFormat, args);
    }
}
