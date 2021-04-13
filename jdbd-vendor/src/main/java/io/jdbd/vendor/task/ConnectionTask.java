package io.jdbd.vendor.task;


import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;

public interface ConnectionTask extends CommunicationTask {

    void addSsl(Consumer<SslWrapper> sslConsumer);

    /**
     * <p>
     * This will invoke :
     *     <ul>
     *         <li>before {@link #start(MorePacketSignal)}</li>
     *         <li>after {@link #decode(ByteBuf, Consumer)},if return value is {@code true}</li>
     *     </ul>
     * </p>
     *
     * @return true:connect database server failure, {@link TaskExecutor} will close channel.
     */
    boolean disconnect();

}
