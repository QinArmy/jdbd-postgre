package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

final class HandshakeV10Task extends MySQLConnectionTask {

    static Mono<HandshakeV10Packet> receive(MySQLTaskAdjutant executorAdjutant) {
        return Mono.create(sink ->
                new HandshakeV10Task(executorAdjutant, sink)
                        .submit(sink::error)

        );
    }

    private final MonoSink<HandshakeV10Packet> sink;

    private HandshakeV10Task(MySQLTaskAdjutant executorAdjutant, MonoSink<HandshakeV10Packet> sink) {
        super(executorAdjutant, -1);
        this.sink = sink;
    }

    @Override
    protected ByteBuf internalStart() {
        // no data send
        return null;
    }

    @Override
    public ByteBuf moreSendPacket() {
        // no data send
        return null;
    }

    @Override
    public Path moreSendFile() {
        // no data send
        return null;
    }

    @Override
    protected boolean internalDecode(ByteBuf cumulateBuffer) {
        if (!PacketUtils.hasOnePacket(cumulateBuffer)) {
            return false;
        }
        int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        int sequenceId = PacketUtils.readInt1(cumulateBuffer);
        int payloadStartIndex = cumulateBuffer.readerIndex();

        if (ErrorPacket.isErrorPacket(cumulateBuffer)) {
            ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer, 0, StandardCharsets.UTF_8);
            this.sink.error(MySQLExceptionUtils.createErrorPacketException(error));

        } else if (sequenceId != 0) {
            this.sink.error(
                    MySQLExceptionUtils.createFatalIoException("Handshake sequenceId[%s] isn't 0 .", sequenceId));
        } else {
            try {
                this.sink.success(HandshakeV10Packet.readHandshake(cumulateBuffer)); //emit HandshakeV10Packet
                cumulateBuffer.readerIndex(payloadStartIndex + payloadLength); // to next packet
            } catch (Throwable e) {
                this.sink.error(MySQLExceptionUtils.createFatalIoException(e, "Handshake packet parse error"));
            }
        }
        return true;
    }


}
