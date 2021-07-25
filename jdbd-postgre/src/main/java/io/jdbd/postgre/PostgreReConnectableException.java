package io.jdbd.postgre;

public class PostgreReConnectableException extends PostgreJdbdException {

    private final boolean reconnect;

    public PostgreReConnectableException(boolean reconnect, String message) {
        super(message);
        this.reconnect = reconnect;
    }

    public PostgreReConnectableException(boolean reconnect, String message, Throwable cause) {
        super(message, cause);
        this.reconnect = reconnect;
    }


    public final boolean isReconnect() {
        return reconnect;
    }


}
