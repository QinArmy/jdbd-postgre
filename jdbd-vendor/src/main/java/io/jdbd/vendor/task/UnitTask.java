package io.jdbd.vendor.task;

import io.jdbd.JdbdException;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;

/**
 * @see CommunicationTask
 */
public abstract class UnitTask<T extends ITaskAdjutant> {


    protected final T adjutant;

    private final CommunicationTask<T> task;

    protected UnitTask(CommunicationTask<T> task) {
        this.task = task;
        this.adjutant = task.adjutant;
    }

    @Nullable
    public Publisher<ByteBuf> start() {
        return null;
    }

    public abstract boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);


    public abstract boolean hasOnePacket(ByteBuf cumulateBuffer);


    protected final void sendPacket(Publisher<ByteBuf> publisher) {
        this.task.sendPacket(publisher);
    }

    protected final void addException(JdbdException e) {
        this.task.addError(e);
    }


}