package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PostgreReConnectableException;

public final class PostgreGssException extends PostgreReConnectableException {


    public PostgreGssException(boolean reconnect, String message) {
        super(reconnect, message);
    }

    public PostgreGssException(boolean reconnect, String message, Throwable cause) {
        super(reconnect, message, cause);
    }


}
