package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.netty.Connection;
import reactor.util.concurrent.Queues;

import java.util.Queue;

final class StatementTaskManager implements CoreSubscriber<ByteBuf> {

    static StatementTaskManager from(Connection connection) {
        StatementTaskManager manager = new StatementTaskManager(connection);
        connection.inbound().receive()
                .retain()// for manager cumulate
                .subscribe(manager);
        return manager;
    }

    private final Queue<StatementTask> receiverQueue = Queues.<StatementTask>small().get();


    private final Connection connection;


    private StatementTaskManager(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void onSubscribe(Subscription s) {

    }

    /*################################## blow CoreSubscriber method ##################################*/

    @Override
    public void onNext(ByteBuf byteBuf) {

    }

    @Override
    public void onError(Throwable t) {

    }

    @Override
    public void onComplete() {

    }


}
