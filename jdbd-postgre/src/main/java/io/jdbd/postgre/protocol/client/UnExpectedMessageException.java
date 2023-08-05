package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;

class UnExpectedMessageException extends JdbdException {

    UnExpectedMessageException(String message) {
        super(message);
    }


}
