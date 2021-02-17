package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.Map;

interface ClientProtocolAdjutant extends ResultRowAdjutant {

    ByteBuf createPacketBuffer(int initialPayloadCapacity);

    ByteBuf createByteBuffer(int initialPayloadCapacity);

    int obtainMaxBytesPerCharClient();

    Charset obtainCharsetClient();

    Charset obtainCharsetResults();

    int obtainNegotiatedCapability();

    Map<Integer, CharsetMapping.CustomCollation> obtainCustomCollationMap();

    ZoneOffset obtainZoneOffsetDatabase();

    ZoneOffset obtainZoneOffsetClient();

    HandshakeV10Packet obtainHandshakeV10Packet();

    HostInfo obtainHostInfo();

    Server obtainServer();

}
