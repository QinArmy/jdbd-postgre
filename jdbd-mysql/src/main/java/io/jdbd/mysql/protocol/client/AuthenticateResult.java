package io.jdbd.mysql.protocol.client;

final class AuthenticateResult {

    private final HandshakeV10Packet handshakeV10Packet;

    private final int negotiatedCapability;

    private AuthenticateResult(HandshakeV10Packet handshakeV10Packet, int negotiatedCapability) {
        this.handshakeV10Packet = handshakeV10Packet;
        this.negotiatedCapability = negotiatedCapability;
    }

    public HandshakeV10Packet handshakeV10Packet() {
        return handshakeV10Packet;
    }

    public int negotiatedCapability() {
        return negotiatedCapability;
    }
}
