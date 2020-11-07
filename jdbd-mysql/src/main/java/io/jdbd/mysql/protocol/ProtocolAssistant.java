package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.conf.HostInfo;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

public interface ProtocolAssistant {

    Charset getHandshakeCharset();

    Charset getPasswordCharset();

    HostInfo getMainHostInfo();

    boolean isUseSsl();

    ByteBuf createPacketBuffer(int initialPayloadCapacity);

    ByteBuf createPayloadBuffer(int initialPayloadCapacity);

    /**
     * @return read-only buffer ,payload length is 1 .
     */
    ByteBuf createOneSizePayload(int payloadByte);

    /**
     * @return read-only buffer ,payload length is 0 .
     */
    ByteBuf createEmptyPayload();

    ServerVersion getServerVersion();

}
