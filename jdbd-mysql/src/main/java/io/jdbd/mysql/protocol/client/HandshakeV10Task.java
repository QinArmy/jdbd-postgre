package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.TaskSignal;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

final class HandshakeV10Task extends MySQLConnectionTask {

    static Mono<HandshakeV10Packet> receive(MySQLTaskAdjutant executorAdjutant) {
        return Mono.create(sink ->
                new HandshakeV10Task(executorAdjutant, sink)
                        .submit(sink::error)

        );
    }

    private static final Logger LOG = LoggerFactory.getLogger(HandshakeV10Task.class);

    private final MonoSink<HandshakeV10Packet> sink;

    private HandshakeV10Task(MySQLTaskAdjutant executorAdjutant, MonoSink<HandshakeV10Packet> sink) {
        super(executorAdjutant, -1);
        this.sink = sink;
    }

    @Override
    protected Publisher<ByteBuf> internalStart(TaskSignal<ByteBuf> signal) {
        LOG.debug("Handshake receive task start");
        // no data send
        return null;
    }

    @Override
    public Publisher<ByteBuf> moreSendPacket() {
        // no data send
        return null;
    }

    @Override
    protected boolean internalDecode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (!PacketUtils.hasOnePacket(cumulateBuffer)) {
            return false;
        }
        int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        int sequenceId = PacketUtils.readInt1(cumulateBuffer);
        int payloadStartIndex = cumulateBuffer.readerIndex();

        if (ErrorPacket.isErrorPacket(cumulateBuffer)) {
            ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer, 0, StandardCharsets.UTF_8);
            this.sink.error(MySQLExceptions.createErrorPacketException(error));

        } else if (sequenceId != 0) {
            this.sink.error(
                    MySQLExceptions.createFatalIoException("Handshake sequenceId[%s] isn't 0 .", sequenceId));
        } else {
            try {
                this.sink.success(HandshakeV10Packet.readHandshake(cumulateBuffer)); //emit HandshakeV10Packet
                cumulateBuffer.readerIndex(payloadStartIndex + payloadLength); // to next packet
            } catch (Throwable e) {
                this.sink.error(MySQLExceptions.createFatalIoException(e, "Handshake packet parse error"));
            }
        }
        return true;
    }


}
