package io.jdbd.vendor.task;

import io.netty.buffer.ByteBuf;
import io.netty.handler.ssl.SslHandler;
import org.reactivestreams.Publisher;
import reactor.netty.tcp.SslProvider;
import reactor.util.annotation.Nullable;

public final class SslWrapper {

    public static SslWrapper create(ConnectionTask currentTask, @Nullable Publisher<ByteBuf> publisher
            , Object sslObject) {
        if (sslObject instanceof SslProvider
                || sslObject instanceof SslHandler) {
            throw new IllegalArgumentException("sslObject error.");
        }
        return new SslWrapper(currentTask, publisher, sslObject);
    }


    private final ConnectionTask currentTask;

    private final Publisher<ByteBuf> publisher;

    private final Object sslObject;

    private SslWrapper(ConnectionTask currentTask, @Nullable Publisher<ByteBuf> publisher, Object sslObject) {
        this.currentTask = currentTask;
        this.publisher = publisher;
        this.sslObject = sslObject;
    }


    public ConnectionTask getCurrentTask() {
        return currentTask;
    }

    @Nullable
    public Publisher<ByteBuf> getPublisher() {
        return publisher;
    }

    public Object getSslObject() {
        return sslObject;
    }


}
