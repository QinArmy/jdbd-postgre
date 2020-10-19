package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.ServerVersion;

final class HandshakeV9Packet extends AbstractHandshakePacket {

    private String  scramble;

    public HandshakeV9Packet(short protocolVersion, ServerVersion serverVersion, long threadId, String scramble) {
        super(protocolVersion, serverVersion, threadId);
        this.scramble = scramble;
    }


}
