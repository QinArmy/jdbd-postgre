package io.jdbd.postgre.protocol.client;

import io.jdbd.vendor.result.FluxResultSink;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;

final class ExtendedQueryTask extends AbstractStmtTask {


    private ExtendedQueryTask(TaskAdjutant adjutant, FluxResultSink sink) {
        super(adjutant, sink, null);
    }


    @Override
    protected final Action onError(Throwable e) {
        return null;
    }

    @Override
    protected final Publisher<ByteBuf> start() {
        return null;
    }

    @Override
    protected final boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        return false;
    }

    @Override
    final void internalToString(StringBuilder builder) {

    }


}
