package io.jdbd.vendor.task;


import io.netty.handler.ssl.SslHandler;

import java.util.function.Consumer;

public interface ConnectionTask extends CommunicationTask {

    void sslHandlerConsumer(Consumer<SslHandler> consumer);

}
