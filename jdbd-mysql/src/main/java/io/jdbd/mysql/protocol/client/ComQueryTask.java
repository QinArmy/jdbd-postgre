package io.jdbd.mysql.protocol.client;

import io.jdbd.MultiResults;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.vendor.DefaultMultiResultsSink;
import io.jdbd.vendor.MultiResultsSink;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.function.Function;


final class ComQueryTask extends AbstractComQueryTask {

    static MultiResults singleCommand(MySQLTaskAdjutant taskAdjutant, String command) {
        Function<MySQLCommunicationTask, ByteBuf> bufFunction = task -> commandBuffer(task, command);
        return new ComQueryTask(taskAdjutant, bufFunction, 1).multiResults;
    }

    static MultiResults multiCommands(MySQLTaskAdjutant taskAdjutant, List<String> commandList) {
        String command = MySQLStringUtils.concat(commandList, ";");
        Function<MySQLCommunicationTask, ByteBuf> bufFunction = task -> commandBuffer(task, command);
        return new ComQueryTask(taskAdjutant, bufFunction, commandList.size()).multiResults;
    }

    private final MultiResultsSink resultsSink;

    private final MultiResults multiResults;


    private ComQueryTask(MySQLTaskAdjutant taskAdjutant, Function<MySQLCommunicationTask, ByteBuf> bufFunction
            , int expectedResultCount) {
        super(taskAdjutant, bufFunction, expectedResultCount);

        DefaultMultiResultsSink resultsSink = DefaultMultiResultsSink.forTask(this, expectedResultCount);
        this.resultsSink = resultsSink;
        this.multiResults = resultsSink.getMultiResults();
    }




    /*################################## blow package template method ##################################*/

    @Override
    void emitError(Throwable e) {
        this.resultsSink.error(e);
    }

    @Override
    void emitUpdateResult(ResultStates resultStates, boolean hasMore) {
        this.resultsSink.nextUpdate(resultStates, hasMore);
    }

    @Override
    boolean emitCurrentQueryRowMeta(ResultRowMeta rowMeta) {
        this.resultsSink.nextQueryRowMeta(rowMeta);
        return false;
    }

    @Override
    MultiResultsSink.RowSink obtainCurrentRowSink() {
        return this.resultsSink.obtainCurrentRowSink();
    }

    @Override
    void emitCurrentRowTerminator(ResultStates resultStates, boolean hasMore) {
        this.resultsSink.emitRowTerminator(resultStates, hasMore);
    }


    /*################################## blow private method ##################################*/


}
