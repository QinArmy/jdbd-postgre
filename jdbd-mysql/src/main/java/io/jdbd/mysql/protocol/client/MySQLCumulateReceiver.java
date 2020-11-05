package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;


interface MySQLCumulateReceiver {

    Mono<ByteBuf> receiveOnePacket();

    Mono<ByteBuf> receiveOne(Function<ByteBuf, ByteBuf> decoder);

    Flux<ByteBuf> receive(BiFunction<ByteBuf, List<ByteBuf>, Boolean> decoder);


}
