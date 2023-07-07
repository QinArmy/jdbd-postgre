package io.jdbd.mysql.protocol;

import io.jdbd.vendor.protocol.DatabaseProtocol;

public interface MySQLProtocol extends DatabaseProtocol {

    /**
     * a single packet max payload byte count.
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html#sect_protocol_basic_packets_sending_mt_16mb">Sending More Than 16Mb</a>
     */
    int MAX_PAYLOAD_SIZE = (1 << 24) - 1;

    long getId();

}
