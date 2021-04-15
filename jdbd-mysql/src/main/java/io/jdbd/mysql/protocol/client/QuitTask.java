package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.task.MorePacketSignal;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.function.Consumer;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_quit.html">Protocol::COM_QUIT</a>
 */
final class QuitTask extends MySQLCommandTask {

    static Mono<Void> quit(MySQLTaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                QuitTask task = new QuitTask(adjutant, sink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }

        });
    }

    private static final Logger LOG = LoggerFactory.getLogger(QuitTask.class);

    private final MonoSink<Void> sink;

    private boolean taskEnd;

    private QuitTask(MySQLTaskAdjutant adjutant, MonoSink<Void> sink) {
        super(adjutant);
        this.sink = sink;
    }


    @Override
    protected Publisher<ByteBuf> internalStart(MorePacketSignal signal) {
        ByteBuf packetBuf = adjutant.createPacketBuffer(1);
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
        int sequenceId = PacketUtils.readInt1AsInt(cumulateBuffer);
        int payloadStartIndex = cumulateBuffer.readerIndex();

        ErrorPacket error;
        error = ErrorPacket.readPacket(cumulateBuffer
                , this.adjutant.obtainNegotiatedCapability(), this.adjutant.obtainCharsetError());
        cumulateBuffer.readerIndex(payloadStartIndex + payloadLength);

        updateSequenceId(sequenceId);

        this.sink.error(MySQLExceptions.createErrorPacketException(error));
        this.taskEnd = true;
        return true;
    }

    @Override
    protected Action internalError(Throwable e) {
        if (this.taskEnd) {
            LOG.error("Unknown error.", e);
        } else {
            this.sink.error(MySQLExceptions.wrap(e));
        }
        return Action.TASK_END;
    }

    @Override
    public void internalOnChannelClose() {
        this.taskEnd = true;
        this.sink.success();
    }


}
