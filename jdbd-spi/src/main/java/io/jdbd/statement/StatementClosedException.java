package io.jdbd.statement;

import io.jdbd.JdbdNonSQLException;

public class StatementClosedException extends JdbdNonSQLException {

    public StatementClosedException(String messageFormat) {
        super(messageFormat);
    }

    public StatementClosedException(Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }

    public StatementClosedException(Throwable cause, boolean enableSuppression, boolean writableStackTrace
            , String messageFormat, Object... args) {
        super(cause, enableSuppression, writableStackTrace, messageFormat, args);
    }
}
