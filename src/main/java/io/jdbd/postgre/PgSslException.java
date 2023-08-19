package io.jdbd.postgre;

public final class PgSslException extends PgReConnectableException {

    public PgSslException(boolean reconnect, String message) {
        super(reconnect, message);
    }

    public PgSslException(boolean reconnect, String message, Throwable cause) {
        super(reconnect, message, cause);
    }


}
