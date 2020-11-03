package io.jdbd.mysql.protocol.client;

public enum ServerStatus {

    SERVER_STATUS_IN_TRANS(1),
    SERVER_STATUS_AUTOCOMMIT(1 << 1),
    SERVER_MORE_RESULTS_EXISTS(1 << 3),
    SERVER_QUERY_NO_GOOD_INDEX_USED(1 << 4),

    SERVER_QUERY_NO_INDEX_USED(1 << 5),
    SERVER_STATUS_CURSOR_EXISTS(1 << 6),
    SERVER_STATUS_LAST_ROW_SENT(1 << 7),
    SERVER_STATUS_DB_DROPPED(1 << 8),

    SERVER_STATUS_NO_BACKSLASH_ESCAPES(1 << 9),
    SERVER_STATUS_METADATA_CHANGED(1 << 10),
    SERVER_QUERY_WAS_SLOW(1 << 11),
    SERVER_PS_OUT_PARAMS(1 << 12),

    SERVER_STATUS_IN_TRANS_READONLY(1 << 13),
    SERVER_SESSION_STATE_CHANGED(1 << 14);


    static ServerStatus fromValue(final int status) {
        for (ServerStatus value : values()) {
            if (status == value.status) {
                return value;
            }
        }
        throw new IllegalArgumentException(String.format("unknown status[%s].", status));
    }

    public final int status;

    ServerStatus(int status) {
        this.status = status;
    }


}
