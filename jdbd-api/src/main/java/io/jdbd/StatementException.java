package io.jdbd;

@Deprecated
public class StatementException extends JdbdNonSQLException {

    public StatementException(String messageFormat, Object... args) {
        super(messageFormat, args);
    }

    public StatementException(Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }
}
