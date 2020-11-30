package io.jdbd;

/**
 * @see MultiResults
 */
public final class NoMoreResultException extends JdbdNonSQLException {

    public NoMoreResultException(String message) {
        super(message);
    }


}
