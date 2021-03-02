package io.jdbd.vendor;

import io.jdbd.JdbdServerIoException;
import io.jdbd.lang.Nullable;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;

/**
 * @param <T> buffer java type,eg:
 *            <ul>
 *                 <li>{@code byte[]}</li>
 *                 <li>{@code java.nio.ByteBuffer}</li>
 *                 <li>{@code io.netty.buffer.ByteBuf}</li>
 *            </ul>
 */
public interface CommunicationTask<T> {

    @Nullable
    Publisher<T> start(TaskSignal signal);

    @Nullable
    TaskPhase getTaskPhase();

    boolean decode(T cumulateBuffer, Consumer<Object> serverStatusConsumer)
            throws JdbdServerIoException;

    @Nullable
    Publisher<T> moreSendPacket();


    @Nullable
    Publisher<T> error(Throwable e);

    void onChannelClose();

    enum TaskPhase {
        SUBMITTED,
        STARTED,
        END
    }
}
