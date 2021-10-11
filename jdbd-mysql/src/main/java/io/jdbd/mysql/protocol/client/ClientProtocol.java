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

    Mono<Void> closeGracefully();

}
