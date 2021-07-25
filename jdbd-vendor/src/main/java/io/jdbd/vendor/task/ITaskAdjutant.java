package io.jdbd.vendor.task;

import io.jdbd.SessionCloseException;
import io.netty.buffer.ByteBufAllocator;

import java.util.Set;
import java.util.function.Consumer;

public interface ITaskAdjutant {

    boolean isActive();

    boolean inEventLoop();

    /**
     * <p>
     * this method is used by {@link CommunicationTask} invoke for submit task to task queue of {@link CommunicationTaskExecutor}.
     * </p>
     *
     * @param errorConsumer invoke errorConsumer when below situation:<ul>
     *                      <li>current thread not in {@link io.netty.channel.EventLoop}</li>
     *                      <li>network channel closed</li>
     *                      <li>task queue reject task</li>
     *                      </ul>
     * @throws IllegalStateException    throw when current thread not in {@link io.netty.channel.EventLoop}
     * @throws IllegalArgumentException throw when {@link CommunicationTask#getTaskPhase()} non-null
     * @throws SessionCloseException    throw then network channel closed
     * @see CommunicationTask#submit(Consumer)
     */
    void syncSubmitTask(CommunicationTask<?> task, Consumer<Void> errorConsumer);

    void execute(Runnable runnable);

    ByteBufAllocator allocator();

    /**
     * @return a unmodifiable set.
     */
    Set<EncryptMode> encryptModes();


}
