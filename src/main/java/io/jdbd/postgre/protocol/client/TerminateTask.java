package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.util.PgExceptions;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.function.Consumer;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Terminate</a>
 */
final class TerminateTask extends PgTask {

    static <T> Mono<T> terminate(TaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                TerminateTask task = new TerminateTask(adjutant, sink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    private final MonoSink<?> sink;

    private TerminateTask(TaskAdjutant adjutant, MonoSink<?> sink) {
        super(adjutant, sink::error);
        this.sink = sink;
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Terminate</a>
     */
    @Override
    protected final Publisher<ByteBuf> start() {
        ByteBuf message = this.adjutant.allocator().buffer(5);
        message.writeByte('X');
        message.writeInt(4);
        return Mono.just(message);
    }

    @Override
    protected final boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        final int msgIndex = cumulateBuffer.readerIndex();
        final byte msgType = cumulateBuffer.getByte(msgIndex);
        final int length;
        if (cumulateBuffer.readableBytes() >= 5) {
            length = cumulateBuffer.getInt(msgIndex + 1);
        } else {
            length = 0;
        }
        String m = String.format("Receive message[type:%s,length:%s] after terminate message.", (char) msgType, length);
        addError(new JdbdException(m));
        publishError(sink::error);
        return true;
    }

    @Override
    protected final Action onError(Throwable e) {
        sink.error(PgExceptions.wrapIfNonJvmFatal(e));
        return Action.TASK_END;
    }

    @Override
    protected final void onChannelClose() {
        this.sink.success();
    }


}
