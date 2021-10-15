package io.jdbd.mysql.protocol.client;


import io.jdbd.mysql.protocol.conf.MyKey;
import reactor.core.publisher.Mono;

/**
 * This interface handle MySQL client/server protocol's connection phase and initialization.
 * <p>
 * Tcp connection should create before create this instance.
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html">Connection Phase</a>
 */
public interface ClientConnectionProtocol {

    /**
     * Do below:
     * <ol>
     *     <li>receive HandshakeV10 packet send by MySQL server.</li>
     *     <li>optionally negotiate ssl for then connection that hold by this instance.</li>
     *     <li>send HandshakeResponse41 packet.</li>
     *     <li>handle more authentication exchange.</li>
     *     <li>configure below url config session group properties:
     *     <ul>
     *          <li>{@link MyKey#sessionVariables}</li>
     *          <li>{@link MyKey#characterEncoding}</li>
     *          <li>{@link MyKey#characterSetResults}</li>
     *          <li>{@link MyKey#connectionCollation}</li>
     *     </ul>
     *     </li>
     *     <li>do initialize ,must contain below operations.
     *     <ul>
     *         <li>{@code set autocommit = 0}</li>
     *         <li>{@code SET SESSION TRANSACTION READ COMMITTED}</li>
     *         <li>more initializing operations</li>
     *     </ul>
     *     </li>
     * </ol>
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41">Protocol::HandshakeResponse41</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_more_data.html">Protocol::AuthMoreData</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html">Protocol::AuthSwitchRequest</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html">Protocol::AuthSwitchResponse</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_old_auth_switch_request.html">Protocol::OldAuthSwitchRequest</a>
     */
    Mono<Void> authenticateAndInitializing();


}
