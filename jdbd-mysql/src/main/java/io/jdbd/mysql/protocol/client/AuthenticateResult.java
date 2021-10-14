package io.jdbd.mysql.protocol.client;

final class AuthenticateResult {

    private final Handshake10 handshake10;

    private final int negotiatedCapability;

    AuthenticateResult(Handshake10 handshake10, int negotiatedCapability) {
        this.handshake10 = handshake10;
        this.negotiatedCapability = negotiatedCapability;
    }

    public Handshake10 handshakeV10Packet() {
        return handshake10;
    }

    public int negotiatedCapability() {
        return negotiatedCapability;
    }
}
