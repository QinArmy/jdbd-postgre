package io.jdbd.vendor;

import reactor.core.publisher.Mono;

public interface CommTaskExecutorAdjutant {


    boolean inEventLoop();

    Mono<Void> submitTask(CommTask<?> task);


}
