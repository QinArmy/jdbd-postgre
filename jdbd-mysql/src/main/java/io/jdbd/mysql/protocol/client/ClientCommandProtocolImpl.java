package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.Connection;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.Map;

public final class ClientCommandProtocolImpl extends AbstractClientProtocol implements ClientCommandProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCommandProtocolImpl.class);


    public static ClientCommandProtocol from(ClientConnectionProtocolImpl connectionProtocol) {
        return new ClientCommandProtocolImpl(connectionProtocol);
    }


    private final MySQLUrl mySQLUrl;

    private final Properties properties;

    private final Connection connection;

    private final MySQLCumulateReceiver cumulateReceiver;

    private final HandshakeV10Packet handshakeV10Packet;


    private final int negotiatedCapability;

    private final Charset charsetClient;

    private final Charset charsetResults;

    private final int maxBytesPerCharClient;

    private final Map<Integer, CharsetMapping.CustomCollation> customCollationMap;

    private final ZoneOffset clientZoneOffset;

    private ClientCommandProtocolImpl(ClientConnectionProtocolImpl connectionProtocol) {
        super(connectionProtocol.connection, connectionProtocol.getMySQLUrl(), connectionProtocol.cumulateReceiver);
        this.mySQLUrl = connectionProtocol.getMySQLUrl();
        this.connection = connectionProtocol.getConnection();
        this.properties = this.mySQLUrl.getHosts().get(0).getProperties();
        this.cumulateReceiver = connectionProtocol.getCumulateReceiver();

        this.handshakeV10Packet = connectionProtocol.getHandshakeV10Packet();
        this.negotiatedCapability = connectionProtocol.getNegotiatedCapability();
        this.charsetClient = connectionProtocol.obtainCharsetClient();
        this.charsetResults = connectionProtocol.obtainCharsetResults();

        this.maxBytesPerCharClient = (int) this.charsetClient.newEncoder().maxBytesPerChar();
        this.customCollationMap = connectionProtocol.obtainCustomCollationMap();
        this.clientZoneOffset = connectionProtocol.obtainClientZoneOffset();
    }

    /*################################## blow ClientCommandProtocol method ##################################*/


    @Override
    public long getId() {
        return this.handshakeV10Packet.getThreadId();
    }

    @Override
    ZoneOffset obtainDatabaseZoneOffset() {
        throw new UnsupportedOperationException();
    }

    @Override
    int obtainNegotiatedCapability() {
        return this.negotiatedCapability;
    }

    @Override
    Charset obtainCharsetClient() {
        return this.charsetClient;
    }

    @Override
    int obtainMaxBytesPerCharClient() {
        return this.maxBytesPerCharClient;
    }

    @Override
    Charset obtainCharsetResults() {
        return this.charsetResults;
    }

    @Override
    Map<Integer, CharsetMapping.CustomCollation> obtainCustomCollationMap() {
        return this.customCollationMap;
    }

    @Override
    public ZoneOffset obtainClientZoneOffset() {
        return this.clientZoneOffset;
    }

    /*################################## blow private method ##################################*/



}
