package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.vendor.conf.HostInfo;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

public interface AuthenticateAssistant {

    Charset getHandshakeCharset();

    Charset getPasswordCharset();

    HostInfo<PropertyKey> getMainHostInfo();

    boolean isUseSsl();

    ByteBuf createPacketBuffer(int initialPayloadCapacity);

    ByteBuf createPayloadBuffer(int initialPayloadCapacity);

    ServerVersion getServerVersion();

}
