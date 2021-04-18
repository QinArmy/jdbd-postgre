package io.jdbd.result;

import io.jdbd.JdbdNonSQLException;

/**
 * @see MultiResult
 */
public final class NoMoreResultException extends JdbdNonSQLException {

    public NoMoreResultException(String messageFormat, Object... args) {
        super(messageFormat, args);
    }
}
