package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.function.BiFunction;


interface MySQLCumulateReceiver {

    Mono<ByteBuf> receiveOnePacket();

    Mono<ByteBuf> receiveOne(BiFunction<ByteBuf, MonoSink<ByteBuf>, Boolean> decoder);

    Flux<ByteBuf> receive(BiFunction<ByteBuf, FluxSink<ByteBuf>, Boolean> decoder);


}
