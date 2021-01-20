package io.jdbd;

public class SQLBindParameterException extends JdbdNonSQLException {

    public SQLBindParameterException(String message) {
        super(message);
    }

    public SQLBindParameterException(String message, Throwable cause) {
        super(message, cause);
    }


}
