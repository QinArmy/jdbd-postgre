package io.jdbd.vendor.task;

import io.jdbd.JdbdException;

final class JdbdConnectionException extends JdbdException {


    public JdbdConnectionException(String message) {
        super(message);
    }

    public JdbdConnectionException(String message, Throwable cause) {
        super(message, cause);
    }


}
