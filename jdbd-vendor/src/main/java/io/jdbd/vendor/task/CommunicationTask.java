package io.jdbd.vendor.task;

import io.jdbd.lang.Nullable;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;


public interface CommunicationTask {

    @Nullable
    Publisher<ByteBuf> start(MorePacketSignal signal);

    @Nullable
    TaskPhase getTaskPhase();

    boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);

    @Nullable
    Publisher<ByteBuf> moreSendPacket();


    Action error(Throwable e);

    void onChannelClose();

    enum TaskPhase {
        SUBMITTED,
        STARTED,
        END
    }

    enum Action {
        MORE_SEND_PACKET,
        TASK_END
    }
}
