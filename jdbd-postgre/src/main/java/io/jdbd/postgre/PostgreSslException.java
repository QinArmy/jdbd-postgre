package io.jdbd.postgre;

public final class PostgreSslException extends PostgreReConnectableException {

    public PostgreSslException(boolean reconnect, String message) {
        super(reconnect, message);
    }

    public PostgreSslException(boolean reconnect, String message, Throwable cause) {
        super(reconnect, message, cause);
    }


}
