package io.jdbd.vendor.task;


import java.util.function.Consumer;

public interface ConnectionTask extends CommunicationTask {

    void connectSignal(Consumer<Object> sslConsumer, Consumer<Void> disconnectConsumer);

}
