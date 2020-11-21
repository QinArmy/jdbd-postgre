package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import reactor.core.publisher.MonoSink;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

abstract class AbstractAuthenticateTask extends MySQLConnectionTask {


    final HandshakeV10Packet handshakeV10Packet;

    final HostInfo hostInfo;

    final Properties properties;

    final MonoSink<Void> sink;

    final Charset handshakeCharset;

    final int negotiatedCapability;

    AbstractAuthenticateTask(MySQLTaskAdjutant executorAdjutant, int sequenceId, MonoSink<Void> sink) {
        super(executorAdjutant, sequenceId);

        this.handshakeV10Packet = executorAdjutant.obtainHandshakeV10Packet();
        this.hostInfo = executorAdjutant.obtainHostInfo();
        this.properties = this.hostInfo.getProperties();
        this.negotiatedCapability = executorAdjutant.obtainNegotiatedCapability();

        this.sink = sink;

        this.handshakeCharset = this.properties.getProperty(PropertyKey.characterEncoding
                , Charset.class, StandardCharsets.UTF_8);
    }


    final byte obtainHandshakeCollationIndex() {
        return CharsetMapping.getHandshakeCollationIndex(
                this.handshakeCharset, this.handshakeV10Packet.getServerVersion());
    }

    /*################################## blow private method ##################################*/


}
