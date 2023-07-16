package io.jdbd.mysql.protocol;

import io.jdbd.mysql.env.MySQLHost;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.nio.charset.Charset;

public interface AuthenticateAssistant {

    Charset getHandshakeCharset();

    Charset getPasswordCharset();

    MySQLHost getHostInfo();

    boolean isUseSsl();

    ByteBuf createPacketBuffer(int initialPayloadCapacity);

    MySQLServerVersion getServerVersion();

    ByteBufAllocator allocator();

}
