package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.SessionEnv;
import io.jdbd.mysql.protocol.conf.MySQLHost0;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.Map;

interface ClientProtocolAdjutant extends ResultRowAdjutant {

    ByteBuf createPacketBuffer(int initialPayloadCapacity);

    ByteBuf createPacketBuffer(int initialPayloadCapacity, int maxCapacity);

    int obtainMaxBytesPerCharClient();

    Charset charsetClient();

    @Nullable
    Charset getCharsetResults();

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-errors.html">Error Message Character Set</a>
     */
    Charset errorCharset();

    Charset obtainCharsetMeta();

    /**
     * @return negotiated capability.
     */
    int capability();

    Map<Integer, Charsets.CustomCollation> obtainCustomCollationMap();

    ZoneOffset serverZone();

    ZoneOffset obtainZoneOffsetClient();

    Handshake10 handshake10();

    ByteBufAllocator allocator();

    @Deprecated
    MySQLHost0 host();

    @Deprecated
    MySQLUrl mysqlUrl();

    SessionEnv obtainServer();

}
