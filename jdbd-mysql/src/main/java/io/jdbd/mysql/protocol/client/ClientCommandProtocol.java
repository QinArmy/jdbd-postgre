package io.jdbd.mysql.protocol.client;


import io.jdbd.mysql.protocol.MySQLPacket;
import reactor.core.publisher.Mono;

public interface ClientCommandProtocol extends ClientProtocol {


    /**
     * @return Text ResultSet ,see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html">Protocol::Text ResultSet</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
     */
    Mono<MySQLPacket> comQueryForResultSet(String sql);


}
