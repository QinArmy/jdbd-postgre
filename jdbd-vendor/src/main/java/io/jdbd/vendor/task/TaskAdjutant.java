package io.jdbd.vendor.task;

import io.netty.buffer.ByteBufAllocator;

import java.util.function.Consumer;

public interface TaskAdjutant {

    boolean inEventLoop();

    void syncSubmitTask(CommunicationTask task, Consumer<Boolean> offerCall) throws IllegalStateException;

    void execute(Runnable runnable);

    ByteBufAllocator allocator();


}
