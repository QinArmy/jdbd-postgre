package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.vendor.EmptyRowSink;
import io.jdbd.vendor.MultiResultsSink;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.nio.charset.Charset;
import java.util.List;
import java.util.function.Function;

final class BatchUpdateTask extends AbstractComQueryTask {

    static Flux<ResultStates> batchCommand(MySQLTaskAdjutant executorAdjutant, List<String> commandList, int totalLength) {
        if (totalLength < 0) {
            throw new JdbdMySQLException("Batch command length too big.");
        }
        final boolean multiQuery;
        multiQuery = (executorAdjutant.obtainNegotiatedCapability() & ClientProtocol.CLIENT_MULTI_STATEMENTS) != 0;

        final Function<MySQLCommunicationTask, ByteBuf> function = task -> {
            ByteBuf byteBuf;
            if (multiQuery) {
                byteBuf = commandBuffer(task, MySQLStringUtils.concat(commandList, ";"));
            } else {
                byteBuf = batchSingleCommand(task, commandList, totalLength);
            }
            return byteBuf;
        };
        return Flux.create(sink -> new BatchUpdateTask(executorAdjutant, function, commandList.size(), sink)
                .submit(sink::error));
    }

    private final FluxSink<ResultStates> sink;

    private MultiResultsSink.RowSink currentRowSink;


    private BatchUpdateTask(MySQLTaskAdjutant taskAdjutant, Function<MySQLCommunicationTask, ByteBuf> bufFunction
            , int expectedResultCount, FluxSink<ResultStates> sink) {
        super(taskAdjutant, bufFunction, expectedResultCount);
        this.sink = sink;
    }

    /*################################## blow packet template method ##################################*/

    @Override
    void emitError(Throwable e) {
        this.sink.error(e);
    }

    @Override
    void emitUpdateResult(ResultStates resultStates, boolean hasMore) {
        this.sink.next(resultStates);
        if (!hasMore) {
            this.sink.complete();
        }
    }

    @Override
    boolean emitCurrentQueryRowMeta(ResultRowMeta rowMeta) {
        if (this.currentRowSink != null) {
            throw new IllegalStateException("this.currentRowSink isn't null.");
        }
        this.currentRowSink = EmptyRowSink.create(rowMeta);
        return true;
    }

    @Override
    MultiResultsSink.RowSink obtainCurrentRowSink() {
        MultiResultsSink.RowSink sink = this.currentRowSink;
        if (sink == null) {
            throw new IllegalStateException("this.currentRowSink is null.");
        }
        return sink;
    }

    @Override
    void emitCurrentRowTerminator(ResultStates resultStates, boolean hasMore) {
        if (this.currentRowSink == null) {
            throw new IllegalStateException("this.currentRowSink isn null.");
        }
        this.currentRowSink = null;
    }

    /*################################## blow private static method ##################################*/

    private static ByteBuf batchSingleCommand(MySQLCommunicationTask task, List<String> commandList
            , final int totalLength) {
        int initialCapacity = task.executorAdjutant.obtainMaxBytesPerCharClient() * totalLength;
        ByteBuf byteBuf = task.executorAdjutant.createPayloadBuffer(initialCapacity);
        final Charset charsetClient = task.executorAdjutant.obtainCharsetClient();
        byte[] payload;
        for (String command : commandList) {
            payload = command.getBytes(charsetClient);
            if (payload.length > ClientProtocol.MAX_PACKET_SIZE) {
                writeBigBuffer(task, byteBuf, payload);
            } else {
                PacketUtils.writeInt3(byteBuf, payload.length);
                byteBuf.writeByte(task.addAndGetSequenceId());
                byteBuf.writeByte(PacketUtils.COM_QUERY_HEADER);
                byteBuf.writeBytes(payload);
            }
        }
        return byteBuf;
    }
}
