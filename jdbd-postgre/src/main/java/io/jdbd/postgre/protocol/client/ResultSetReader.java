package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;

/**
 * @see io.jdbd.vendor.result.ResultSink
 */
interface ResultSetReader {

    boolean read(ByteBuf cumulateBuffer, Consumer<Object> serverStatesConsumer) throws JdbdException;

}
