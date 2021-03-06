package io.jdbd.vendor.task;


import io.netty.handler.ssl.SslHandler;

import java.util.function.Consumer;

public interface AuthenticateTask extends CommunicationTask {

    void sslHandlerConsumer(Consumer<SslHandler> consumer);

}
