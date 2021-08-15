package io.jdbd.vendor.result;

import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;

/**
 * @see ResultSink
 */
public interface ResultSetReader {

    boolean read(ByteBuf cumulateBuffer, Consumer<Object> statesConsumer);

    boolean isResettable();

}
