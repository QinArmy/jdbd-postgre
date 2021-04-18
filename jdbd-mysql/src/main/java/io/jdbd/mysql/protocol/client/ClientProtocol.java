package io.jdbd.mysql.protocol.client;

import reactor.core.publisher.Mono;

interface ClientProtocol {

    /**
     * a single packet max payload byte count.
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html#sect_protocol_basic_packets_sending_mt_16mb">Sending More Than 16Mb</a>
     */
    int MAX_PAYLOAD_SIZE = (1 << 24) - 1;

    int SERVER_STATUS_IN_TRANS = 1;
    int SERVER_STATUS_AUTOCOMMIT = 2; // Server in auto_commit mode
    int SERVER_MORE_RESULTS_EXISTS = 8; // Multi query - next query exists
    int SERVER_QUERY_NO_GOOD_INDEX_USED = 1 << 4;

    int SERVER_QUERY_NO_INDEX_USED = 1 << 5;
    int SERVER_STATUS_CURSOR_EXISTS = 1 << 6;
    int SERVER_STATUS_LAST_ROW_SENT = 1 << 7; // The server status for 'last-row-sent'
    int SERVER_QUERY_WAS_SLOW = 1 << 11;

    int SERVER_SESSION_STATE_CHANGED = 1 << 14;

    // below Capabilities Flags https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
    int CLIENT_LONG_PASSWORD = 1; /* new more secure passwords */
    int CLIENT_FOUND_ROWS = 1 << 1;
    int CLIENT_LONG_FLAG = 1 << 2; /* Get all column flags */
    int CLIENT_CONNECT_WITH_DB = 1 << 3;
    int CLIENT_COMPRESS = 1 << 5; /* Can use compression protcol */
    int CLIENT_LOCAL_FILES = 1 << 7; /* Can use LOAD DATA LOCAL */
    int CLIENT_PROTOCOL_41 = 1 << 9; // for > 4.1.1
    int CLIENT_INTERACTIVE = 1 << 10;
    int CLIENT_SSL = 1 << 11;
    int CLIENT_TRANSACTIONS = 1 << 13; // Client knows about transactions
    int CLIENT_SECURE_CONNECTION = 1 << 15;
    int CLIENT_MULTI_STATEMENTS = 1 << 16; // Enable/disable multiquery support
    int CLIENT_MULTI_RESULTS = 1 << 17; // Enable/disable multi-results
    int CLIENT_PS_MULTI_RESULTS = 1 << 18; // Enable/disable multi-results for server prepared statements
    int CLIENT_PLUGIN_AUTH = 1 << 19;
    int CLIENT_CONNECT_ATTRS = 1 << 20;
    int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21;
    int CLIENT_CAN_HANDLE_EXPIRED_PASSWORD = 1 << 22;
    int CLIENT_SESSION_TRACK = 1 << 23;
    int CLIENT_DEPRECATE_EOF = 1 << 24;

    int CLIENT_OPTIONAL_RESULTSET_METADATA = 1 << 25;
    int CLIENT_QUERY_ATTRIBUTES = 1 << 27;
    int CLIENT_SSL_VERIFY_SERVER_CERT = 1 << 30;

    Mono<Void> closeGracefully();

}
