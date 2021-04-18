package io.jdbd.result;

import io.jdbd.JdbdNonSQLException;

/**
 * @see MultiResults
 */
public final class NoMoreResultException extends JdbdNonSQLException {

    public NoMoreResultException(String messageFormat, Object... args) {
        super(messageFormat, args);
    }
}
