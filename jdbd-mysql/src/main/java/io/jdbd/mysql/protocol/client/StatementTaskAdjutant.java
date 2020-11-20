package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.vendor.CommTaskExecutorAdjutant;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.Map;

public interface StatementTaskAdjutant extends ResultRowAdjutant, CommTaskExecutorAdjutant {

    ByteBuf createPacketBuffer(int initialPayloadCapacity);

    void taskTerminate(ByteBufStatementTask task);

    int obtainMaxBytesPerCharClient();

    Charset obtainCharsetClient();

    Charset obtainCharsetResults();

    int obtainNegotiatedCapability();

    void submitTask(ByteBufStatementTask task);

    boolean inEventLoop();

    boolean executeInEventLoop(Runnable task);

    Map<Integer, CharsetMapping.CustomCollation> obtainCustomCollationMap();

    ZoneOffset obtainZoneOffsetDatabase();

    ZoneOffset obtainZoneOffsetClient();

    Properties obtainProperties();

}
