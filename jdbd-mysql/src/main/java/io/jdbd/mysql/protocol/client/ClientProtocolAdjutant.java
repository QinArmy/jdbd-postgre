package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.vendor.conf.HostInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.Map;

interface ClientProtocolAdjutant extends ResultRowAdjutant {

    ByteBuf createPacketBuffer(int initialPayloadCapacity);

    int obtainMaxBytesPerCharClient();

    Charset obtainCharsetClient();

    @Nullable
    Charset getCharsetResults();

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-errors.html">Error Message Character Set</a>
     */
    Charset obtainCharsetError();

    Charset obtainCharsetMeta();

    int obtainNegotiatedCapability();

    Map<Integer, CharsetMapping.CustomCollation> obtainCustomCollationMap();

    ZoneOffset obtainZoneOffsetDatabase();

    ZoneOffset obtainZoneOffsetClient();

    HandshakeV10Packet obtainHandshakeV10Packet();

    ByteBufAllocator allocator();

    HostInfo<PropertyKey> obtainHostInfo();

    Server obtainServer();

}
