package io.jdbd.vendor.task;

import io.jdbd.JdbdNonSQLException;

final class JdbdConnectionException extends JdbdNonSQLException {


    public JdbdConnectionException(String message) {
        super(message);
    }

    public JdbdConnectionException(String message, Throwable cause) {
        super(message, cause);
    }


}
