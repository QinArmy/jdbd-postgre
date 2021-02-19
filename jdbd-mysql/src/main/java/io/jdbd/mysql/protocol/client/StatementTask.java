package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import reactor.core.publisher.FluxSink;

import java.util.function.Consumer;

interface StatementTask {

    int obtainStatementId();

    MySQLColumnMeta[] obtainParameterMetas();

    ClientProtocolAdjutant obtainAdjutant();

    int addAndGetSequenceId();

    int updateSequenceId(int sequenceId);

    void handleWriteCommandError(Throwable e);

    void handleReadResultSetError(Throwable e);

    boolean isFetchResult();

    boolean returnResultSet();

    FluxSink<ResultRow> obtainRowSink() throws IllegalStateException;

    Consumer<ResultStates> obtainStatesConsumer() throws IllegalStateException;

    /**
     * for {@link ResultSetReader}
     */
    boolean hasError();

}
