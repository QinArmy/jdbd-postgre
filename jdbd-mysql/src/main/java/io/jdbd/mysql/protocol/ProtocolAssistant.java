package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.conf.HostInfo;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

public interface ProtocolAssistant {

    Charset getClientCharset();

    Charset getPasswordCharset();

    HostInfo getMainHostInfo();

    boolean isUseSsl();

    ByteBuf createPacketBuffer(int payloadCapacity);

    ByteBuf createEmptyPacketForWrite();

}
