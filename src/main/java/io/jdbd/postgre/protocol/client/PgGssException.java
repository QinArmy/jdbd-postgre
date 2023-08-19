package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgReConnectableException;

public final class PgGssException extends PgReConnectableException {


    public PgGssException(boolean reconnect, String message) {
        super(reconnect, message);
    }

    public PgGssException(boolean reconnect, String message, Throwable cause) {
        super(reconnect, message, cause);
    }


}
