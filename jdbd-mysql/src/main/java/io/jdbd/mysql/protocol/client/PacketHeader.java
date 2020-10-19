package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.ProtocolDataUtils;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html#sect_protocol_basic_packets_packet">Protocol::Packet</a>
 */
public final class PacketHeader {

    public static PacketHeader readHeader(byte[] bytes, int offset) {
        if (offset > -1 && offset < bytes.length) {
            return new PacketHeader(ProtocolDataUtils.readInt3(bytes,offset)
                    ,ProtocolDataUtils.readInt1(bytes,offset + 3));
        }
        throw ProtocolDataUtils.createIndexOutOfBoundsException(bytes.length,offset);
    }

    private final int payloadLength;

    private final short sequenceId;

     PacketHeader(int payloadLength, short sequenceId) {
        this.payloadLength = payloadLength;
        this.sequenceId = sequenceId;
    }

    public int getPayloadLength() {
        return this.payloadLength;
    }

    public short getSequenceId() {
        return this.sequenceId;
    }
}
