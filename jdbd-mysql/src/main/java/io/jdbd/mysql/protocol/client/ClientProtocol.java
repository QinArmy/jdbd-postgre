package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;
import reactor.core.publisher.Mono;

public interface ClientProtocol {

    int MAX_PACKET_SIZE = (1 << 24) - 1;

    int SERVER_STATUS_IN_TRANS = 1;
    int SERVER_STATUS_AUTOCOMMIT = 2; // Server in auto_commit mode
    int SERVER_MORE_RESULTS_EXISTS = 8; // Multi query - next query exists
    int SERVER_QUERY_NO_GOOD_INDEX_USED = 16;
    int SERVER_QUERY_NO_INDEX_USED = 32;
    int SERVER_STATUS_CURSOR_EXISTS = 64;
    int SERVER_STATUS_LAST_ROW_SENT = 128; // The server status for 'last-row-sent'
    int SERVER_QUERY_WAS_SLOW = 1 << 11;

    int CLIENT_LONG_PASSWORD = 0x00000001; /* new more secure passwords */
    int CLIENT_FOUND_ROWS = 0x00000002;
    int CLIENT_LONG_FLAG = 0x00000004; /* Get all column flags */
    int CLIENT_CONNECT_WITH_DB = 0x00000008;
    int CLIENT_COMPRESS = 0x00000020; /* Can use compression protcol */
    int CLIENT_LOCAL_FILES = 0x00000080; /* Can use LOAD DATA LOCAL */
    int CLIENT_PROTOCOL_41 = 1 << 9; // for > 4.1.1
    int CLIENT_INTERACTIVE = 0x00000400;
    int CLIENT_SSL = 0x00000800;
    int CLIENT_TRANSACTIONS = 0x00002000; // Client knows about transactions
    int CLIENT_RESERVED = 0x00004000; // for 4.1.0 only
    int CLIENT_SECURE_CONNECTION = 0x00008000;
    int CLIENT_MULTI_STATEMENTS = 0x00010000; // Enable/disable multiquery support
    int CLIENT_MULTI_RESULTS = 0x00020000; // Enable/disable multi-results
    int CLIENT_PS_MULTI_RESULTS = 0x00040000; // Enable/disable multi-results for server prepared statements
    int CLIENT_PLUGIN_AUTH = 0x00080000;
    int CLIENT_CONNECT_ATTRS = 0x00100000;
    int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21;
    int CLIENT_CAN_HANDLE_EXPIRED_PASSWORD = 0x00400000;
    int CLIENT_SESSION_TRACK = 0x00800000;
    int CLIENT_DEPRECATE_EOF = 0x01000000;


    Mono<MySQLPacket> handshake();

    Mono<MySQLPacket> responseHandshake();

    Mono<MySQLPacket> sslRequest();

}
