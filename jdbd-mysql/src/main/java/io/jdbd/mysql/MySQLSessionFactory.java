package io.jdbd.mysql;

import io.jdbd.JdbdSession;
import io.jdbd.JdbdSessionFactory;
import io.jdbd.mysql.protocol.client.ClientCommandProtocol;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import reactor.core.publisher.Mono;

import java.util.Map;

public class MySQLSessionFactory implements JdbdSessionFactory {

    public static MySQLSessionFactory getInstance(String url, Map<String, String> properties) {
        return new MySQLSessionFactory(MySQLUrl.getInstance(url, properties));
    }

    private final MySQLUrl mySQLUrl;

    private MySQLSessionFactory(MySQLUrl mySQLUrl) {
        this.mySQLUrl = mySQLUrl;
    }

    @Override
    public Mono<JdbdSession> getSession() {
        Mono<JdbdSession> mono;
        switch (this.mySQLUrl.getProtocol()) {
            case SINGLE_CONNECTION:
                mono = createClientSession();
                break;
            case XDEVAPI_SESSION:
            case FAILOVER_CONNECTION:
            case LOADBALANCE_CONNECTION:
            case REPLICATION_CONNECTION:
            case XDEVAPI_DNS_SRV_SESSION:
            case FAILOVER_DNS_SRV_CONNECTION:
            case LOADBALANCE_DNS_SRV_CONNECTION:
            case REPLICATION_DNS_SRV_CONNECTION:
            default:
                mono = Mono.empty();
        }
        return mono;
    }

    @Override
    public Mono<Void> close() {
        return Mono.empty();
    }

    private Mono<JdbdSession> createClientSession() {
        return Mono.empty();
    }

    private Mono<ClientCommandProtocol> handshake(ClientCommandProtocol clientProtocol) {
        return Mono.empty();
    }

    private Mono<ClientCommandProtocol> sslRequest(ClientCommandProtocol clientProtocol) {
        return Mono.just(clientProtocol);
    }

    private Mono<ClientCommandProtocol> authenticate(ClientCommandProtocol clientProtocol) {
        return Mono.just(clientProtocol);
    }

}
