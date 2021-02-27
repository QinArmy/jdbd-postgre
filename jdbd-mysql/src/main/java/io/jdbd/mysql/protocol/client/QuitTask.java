package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.TaskSignal;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.function.Consumer;

final class QuitTask extends MySQLConnectionTask {

    static Mono<Void> quit(MySQLTaskAdjutant executorAdjutant) {
        return Mono.create(sink ->
                new QuitTask(executorAdjutant, sink)
                        .submit(sink::error)

        );
    }

    private final MonoSink<Void> sink;

    private QuitTask(MySQLTaskAdjutant executorAdjutant, MonoSink<Void> sink) {
        super(executorAdjutant, -1);
        this.sink = sink;
    }


    @Override
    protected Publisher<ByteBuf> internalStart(TaskSignal<ByteBuf> signal) {
        ByteBuf packetBuf = executorAdjutant.createPacketBuffer(1);
        packetBuf.writeByte(PacketUtils.COM_QUIT_HEADER);
        PacketUtils.writePacketHeader(packetBuf, addAndGetSequenceId());
        return Mono.just(packetBuf);
    }

    @Override
    protected boolean internalDecode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (!PacketUtils.hasOnePacket(cumulateBuffer)) {
            return false;
        }
        int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        int sequenceId = PacketUtils.readInt1(cumulateBuffer);
        int payloadStartIndex = cumulateBuffer.readerIndex();

        ErrorPacket error;
        error = ErrorPacket.readPacket(cumulateBuffer
                , this.executorAdjutant.obtainNegotiatedCapability(), this.executorAdjutant.obtainCharsetResults());
        cumulateBuffer.readerIndex(payloadStartIndex + payloadLength);

        updateSequenceId(sequenceId);

        this.sink.error(MySQLExceptions.createErrorPacketException(error));
        return true;
    }


    @Override
    public void internalOnChannelClose() {
        this.sink.success();
    }

}
