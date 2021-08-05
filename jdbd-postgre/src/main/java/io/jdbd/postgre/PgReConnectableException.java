package io.jdbd.postgre;

public class PgReConnectableException extends PgJdbdException {

    private final boolean reconnect;

    public PgReConnectableException(boolean reconnect, String message) {
        super(message);
        this.reconnect = reconnect;
    }

    public PgReConnectableException(boolean reconnect, String message, Throwable cause) {
        super(message, cause);
        this.reconnect = reconnect;
    }


    public final boolean isReconnect() {
        return reconnect;
    }


}
