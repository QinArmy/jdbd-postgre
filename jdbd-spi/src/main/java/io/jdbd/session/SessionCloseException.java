package io.jdbd.session;

import io.jdbd.JdbdException;

public final class SessionCloseException extends JdbdException {


    public SessionCloseException(String message) {
        super(message);
    }


}
