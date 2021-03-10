package io.jdbd.vendor.task;


import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

public interface ConnectionTask extends CommunicationTask {

    void connectSignal(Function<Object, Mono<Void>> sslFunction);

    /**
     * <p>
     * This will invoke :
     *     <ul>
     *         <li>before {@link #start(MorePacketSignal)}</li>
     *         <li>after {@link #decode(ByteBuf, Consumer)},if return value is {@code true}</li>
     *         <li>after {@link #onSendSuccess()} ,if return value is {@code false}</li>
     *     </ul>
     * </p>
     *
     * @return true:connect database server failure, {@link TaskExecutor} will close channel.
     */
    boolean disconnect();

}
