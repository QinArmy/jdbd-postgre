package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;

class UnExpectedMessageException extends PgJdbdException {

    UnExpectedMessageException(String message) {
        super(message);
    }


}
