package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.MonoSink;

import java.util.function.BiFunction;

abstract class CommandQueryDecoder {

    static BiFunction<ByteBuf, MonoSink<ByteBuf>, Boolean> updateDecoder(DecoderAdjutant adjutant) {
        return null;
    }


    private final DecoderAdjutant adjutant;

    private CommandQueryDecoder(DecoderAdjutant adjutant) {
        this.adjutant = adjutant;
    }


}
