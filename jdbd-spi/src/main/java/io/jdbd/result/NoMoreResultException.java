package io.jdbd.result;

import io.jdbd.JdbdException;

/**
 * @see MultiResult
 */
public final class NoMoreResultException extends JdbdException {

    public NoMoreResultException(String messageFormat, Object... args) {
        super(messageFormat, args);
    }
}
