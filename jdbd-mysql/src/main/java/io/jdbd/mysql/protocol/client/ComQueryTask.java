package io.jdbd.mysql.protocol.client;

import io.jdbd.*;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.stmt.BatchBindWrapper;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.stmt.BindableWrapper;
import io.jdbd.mysql.stmt.StmtWrappers;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.result.*;
import io.jdbd.vendor.stmt.ParamWrapper;
import io.jdbd.vendor.task.TaskSignal;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * <p>
 * below is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
 */
final class ComQueryTask extends MySQLCommandTask {

    /**
     * @see #ComQueryTask(String, MonoSink, MySQLTaskAdjutant)
     */
    static Mono<ResultStates> update(final String sql, final MySQLTaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(sql, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    /**
     * @see #ComQueryTask(String, FluxSink, Consumer, MySQLTaskAdjutant)
     */
    static Flux<ResultRow> query(final String sql, Consumer<ResultStates> statesConsumer, MySQLTaskAdjutant adjutant) {
        return Flux.create(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(sql, sink, statesConsumer, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    /**
     * @see #ComQueryTask(List, FluxSink, MySQLTaskAdjutant)
     */
    static Flux<ResultStates> batchUpdate(final List<String> sqlList, final MySQLTaskAdjutant adjutant) {
        final Flux<ResultStates> flux;
        if (sqlList.isEmpty()) {
            flux = Flux.error(MySQLExceptions.createEmptySqlException());
        } else {
            flux = Flux.create(sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(sqlList, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }

            });
        }
        return flux;
    }

    /**
     * @see #ComQueryTask(BindableWrapper, MonoSink, MySQLTaskAdjutant)
     * @see ComPreparedTask#update(ParamWrapper, MySQLTaskAdjutant)
     */
    static Mono<ResultStates> bindableUpdate(final BindableWrapper wrapper, final MySQLTaskAdjutant adjutant) {
        Mono<ResultStates> mono;
        Properties<PropertyKey> properties = adjutant.obtainHostInfo().getProperties();
        if (properties.getOrDefault(PropertyKey.useServerPrepStmts, Boolean.class)
                || BindUtils.hasLongData(wrapper.getParamGroup())) {
            // has long data ,can't use client prepare statement.
            mono = ComPreparedTask.update(wrapper, adjutant);
        } else {
            mono = Mono.create(sink -> {
                ComQueryTask task;
                try {
                    task = new ComQueryTask(wrapper, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }

            });
        }
        return mono;
    }

    /**
     * @see #ComQueryTask(BatchBindWrapper, FluxSink, MySQLTaskAdjutant)
     */
    static Flux<ResultStates> bindableBatch(final BatchBindWrapper wrapper, final MySQLTaskAdjutant adjutant) {
        final List<List<BindValue>> parameterGroupList = wrapper.getParamGroupList();
        Properties<PropertyKey> properties = adjutant.obtainHostInfo().getProperties();
        final Flux<ResultStates> flux;
        if (parameterGroupList.size() > 1000 //TODO decide max size by PropertyKey.maxAllowedPacket
                || properties.getOrDefault(PropertyKey.useServerPrepStmts, Boolean.class)
                || BindUtils.hasLongDataGroup(parameterGroupList)) {
            // has long data ,can't use client prepare statement.
            flux = ComPreparedTask.batchUpdate(wrapper, adjutant);
        } else {
            flux = Flux.create(sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(wrapper, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }
            });
        }

        return flux;
    }

    /**
     * <p>
     * this method create task for client query prepare statement.
     * </p>
     *
     * @see #ComQueryTask(BindableWrapper, FluxSink, MySQLTaskAdjutant)
     */
    static Flux<ResultRow> bindableQuery(final BindableWrapper wrapper, final MySQLTaskAdjutant adjutant) {
        final Flux<ResultRow> flux;
        Properties<PropertyKey> properties = adjutant.obtainHostInfo().getProperties();
        if (properties.getOrDefault(PropertyKey.useServerPrepStmts, Boolean.class)
                || BindUtils.hasLongData(wrapper.getParamGroup())) {
            // has long data ,can't use client prepare statement.
            flux = ComPreparedTask.query(wrapper, adjutant);
        } else {
            flux = Flux.create(sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(wrapper, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }
            });
        }

        return flux;
    }


    /**
     * <p>
     * this method create task for multi statement.
     * </p>
     *
     * @see #ComQueryTask(List, MultiResultsSink, MySQLTaskAdjutant)
     */
    static ReactorMultiResults bindableMultiStmt(final List<BindableWrapper> bindableWrapperList
            , final MySQLTaskAdjutant adjutant) {
        final ReactorMultiResults multiResults;
        if (bindableWrapperList.isEmpty()) {
            multiResults = JdbdMultiResults.error(MySQLExceptions.createEmptySqlException());
        } else if (Capabilities.supportMultiStatement(adjutant.obtainNegotiatedCapability())) {
            multiResults = JdbdMultiResults.create(adjutant, sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(bindableWrapperList, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }

            });
        } else {
            multiResults = JdbdMultiResults.error(MySQLExceptions.notSupportMultiStatementException());
        }
        return multiResults;
    }

    /**
     * @see #ComQueryTask(MultiResultsSink, List, MySQLTaskAdjutant)
     */
    static ReactorMultiResults multiStmt(final List<String> sqlList, final MySQLTaskAdjutant adjutant) {
        ReactorMultiResults multiResults;
        if (sqlList.isEmpty()) {
            multiResults = JdbdMultiResults.error(MySQLExceptions.createEmptySqlException());
        } else if (Capabilities.supportMultiStatement(adjutant.obtainNegotiatedCapability())) {
            multiResults = JdbdMultiResults.create(adjutant, sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(sink, sqlList, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }
            });
        } else {
            multiResults = JdbdMultiResults.error(MySQLExceptions.notSupportMultiStatementException());
        }
        return multiResults;
    }


    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTask.class);

    private final DownstreamSink downstreamSink;

    private final Mode mode;

    private final int sqlCount;

    private TempMultiStmtStatus tempMultiStmtStatus;

    /**
     * {@link #updateLastResultStates(int, ResultStates)} can update this filed.
     */
    private int currentResultSequenceId = 1;

    private Phase phase;

    private Pair<Integer, ResultStates> lastResultStates;

    private List<JdbdException> errorList;

    private ResultSetReader dirtyResultSetReader;

    /**
     * @see #update(String, MySQLTaskAdjutant)
     */
    private ComQueryTask(final String sql, MonoSink<ResultStates> sink, MySQLTaskAdjutant adjutant)
            throws SQLException {
        super(adjutant);
        this.sqlCount = 1;
        this.mode = Mode.SINGLE_STMT;
        this.packetPublisher = ComQueryCommandWriter.createStaticSingleCommand(sql, this::addAndGetSequenceId
                , adjutant);
        this.downstreamSink = new SingleUpdateSink(sink);
    }

    /**
     * @see #query(String, Consumer, MySQLTaskAdjutant)
     */
    private ComQueryTask(final String sql, FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer
            , MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);
        LOG.trace("create single statement query task.");
        this.sqlCount = 1;
        this.mode = Mode.SINGLE_STMT;
        this.packetPublisher = ComQueryCommandWriter.createStaticSingleCommand(sql, this::addAndGetSequenceId
                , adjutant);

        this.downstreamSink = new SingleQuerySink(sink, statesConsumer);
    }

    /**
     * @see #batchUpdate(List, MySQLTaskAdjutant)
     */
    private ComQueryTask(final List<String> sqlList, final FluxSink<ResultStates> resultSink
            , MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);
        this.sqlCount = sqlList.size();

        if (Capabilities.supportMultiStatement(this.negotiatedCapability)) {
            this.mode = Mode.MULTI_STMT;
            this.packetPublisher = ComQueryCommandWriter.createStaticMultiCommand(sqlList, this::addAndGetSequenceId
                    , adjutant);
            this.downstreamSink = new MultiStatementBatchUpdateSink(resultSink);
        } else if (this.sqlCount > 3) {
            this.mode = Mode.TEMP_MULTI;
            this.packetPublisher = ComQueryCommandWriter.createStaticMultiCommand(sqlList, this::addAndGetSequenceId
                    , adjutant);
            this.downstreamSink = new MultiStatementBatchUpdateSink(resultSink);
        } else {
            this.mode = Mode.SINGLE_STMT;
            this.packetPublisher = ComQueryCommandWriter.createStaticSingleCommand(sqlList.get(0)
                    , this::addAndGetSequenceId, adjutant);
            this.downstreamSink = new SingleStatementBatchUpdate(sqlList, resultSink);
        }

    }

    /**
     * <p>
     * this method create task for client update prepare statement.
     * </p>
     *
     * @see #bindableUpdate(BindableWrapper, MySQLTaskAdjutant)
     */
    private ComQueryTask(final BindableWrapper wrapper, final MonoSink<ResultStates> resultSink
            , MySQLTaskAdjutant adjutant) throws SQLException, LongDataReadException {
        super(adjutant);
        this.sqlCount = 1;
        this.mode = Mode.SINGLE_STMT;
        final List<ByteBuf> packetList = ComQueryCommandWriter.createBindableCommand(
                wrapper, this::addAndGetSequenceId, adjutant);
        this.packetPublisher = Flux.fromIterable(packetList);
        this.downstreamSink = new SingleUpdateSink(resultSink);

    }

    /**
     * <p>
     * this method create task for client query prepare statement.
     * </p>
     *
     * @see #bindableQuery(BindableWrapper, MySQLTaskAdjutant)
     */
    private ComQueryTask(final BindableWrapper wrapper, final FluxSink<ResultRow> sink
            , final MySQLTaskAdjutant adjutant) throws SQLException, LongDataReadException {
        super(adjutant);
        this.sqlCount = 1;
        this.mode = Mode.SINGLE_STMT;
        final List<ByteBuf> packetList = ComQueryCommandWriter.createBindableCommand(
                wrapper, this::addAndGetSequenceId, adjutant);
        this.packetPublisher = Flux.fromIterable(packetList);
        this.downstreamSink = new SingleQuerySink(sink, wrapper.getStatesConsumer());
    }

    /**
     * <p>
     * this method create task for prepare batch update statement.
     * </p>
     *
     * @see #bindableBatch(BatchBindWrapper, MySQLTaskAdjutant)
     */
    private ComQueryTask(final BatchBindWrapper wrapper, final FluxSink<ResultStates> sink
            , final MySQLTaskAdjutant adjutant) throws SQLException, LongDataReadException {
        super(adjutant);
        final List<List<BindValue>> parameterGroupList = wrapper.getParamGroupList();
        this.sqlCount = parameterGroupList.size();

        final List<ByteBuf> packetList;
        if (Capabilities.supportMultiStatement(this.negotiatedCapability)) {
            this.mode = Mode.MULTI_STMT;
            packetList = ComQueryCommandWriter.createBindableBatchCommand(wrapper, this::addAndGetSequenceId, adjutant);
            this.downstreamSink = new MultiStatementBatchUpdateSink(sink);
        } else if (this.sqlCount > 3) {
            this.mode = Mode.TEMP_MULTI;
            packetList = ComQueryCommandWriter.createBindableBatchCommand(wrapper, this::addAndGetSequenceId, adjutant);
            this.downstreamSink = new MultiStatementBatchUpdateSink(sink);
        } else {
            this.mode = Mode.SINGLE_STMT;
            BindableWrapper bindableWrapper = StmtWrappers.multi(wrapper.getSql(), parameterGroupList.get(0));
            packetList = ComQueryCommandWriter.createBindableCommand(bindableWrapper, this::addAndGetSequenceId
                    , adjutant);
            this.downstreamSink = new BindableSingleStatementBatchUpdate(wrapper, sink);
        }

        this.packetPublisher = Flux.fromIterable(packetList);
    }


    /**
     * <p>
     * this method create task for multi statement.
     * </p>
     *
     * @see #bindableMultiStmt(List, MySQLTaskAdjutant)
     */
    private ComQueryTask(final List<BindableWrapper> bindableWrapperList, final MultiResultsSink resultSink
            , final MySQLTaskAdjutant adjutant)
            throws SQLException, LongDataReadException {
        super(adjutant);
        if (!Capabilities.supportMultiStatement(this.negotiatedCapability)) {
            throw MySQLExceptions.createMultiStatementError();
        }

        final List<ByteBuf> packetList = ComQueryCommandWriter.createBindableMultiCommand(
                bindableWrapperList, this::addAndGetSequenceId, adjutant);

        this.packetPublisher = Flux.fromIterable(packetList);
        this.sqlCount = bindableWrapperList.size();
        this.mode = Mode.MULTI_STMT;
        this.downstreamSink = new MultiStmtSink(resultSink);

    }

    /**
     * @see #multiStmt(List, MySQLTaskAdjutant)
     */
    private ComQueryTask(final MultiResultsSink resultSink, final List<String> sqlList
            , final MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);
        if (!Capabilities.supportMultiStatement(this.negotiatedCapability)) {
            throw MySQLExceptions.createMultiStatementError();
        }
        this.packetPublisher = ComQueryCommandWriter.createStaticMultiCommand(sqlList, this::addAndGetSequenceId
                , adjutant);
        this.sqlCount = sqlList.size();
        this.mode = Mode.MULTI_STMT;
        this.downstreamSink = new MultiStmtSink(resultSink);
    }


    /*################################## blow package template method ##################################*/

    @Override
    protected Publisher<ByteBuf> internalStart(TaskSignal signal) {
        final Publisher<ByteBuf> publisher;
        if (this.mode == Mode.TEMP_MULTI) {
            this.phase = Phase.READ_MULTI_STMT_ENABLE_RESULT;
            publisher = Mono.just(createSetOptionPacket(true));
        } else {
            this.phase = Phase.READ_RESPONSE_RESULT_SET;
            publisher = Objects.requireNonNull(this.packetPublisher, "this.packetPublisher");
            this.packetPublisher = null;
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("send COM_QUERY packet with mode[{}]", this.mode);
        }
        return publisher;
    }

    @Override
    protected boolean internalDecode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        boolean taskEnd = false;
        boolean continueRead = true;
        while (continueRead) {
            switch (this.phase) {
                case READ_RESPONSE_RESULT_SET: {
                    taskEnd = readResponseResultSet(cumulateBuffer, serverStatusConsumer);
                    continueRead = !taskEnd && PacketUtils.hasOnePacket(cumulateBuffer);
                }
                break;
                case READ_TEXT_RESULT_SET: {
                    taskEnd = readTextResultSet(cumulateBuffer, serverStatusConsumer);
                    continueRead = !taskEnd && PacketUtils.hasOnePacket(cumulateBuffer);
                }
                break;
                case READ_MULTI_STMT_ENABLE_RESULT: {
                    taskEnd = readEnableMultiStmtResponse(cumulateBuffer, serverStatusConsumer);
                    if (!taskEnd) {
                        this.phase = Phase.READ_RESPONSE_RESULT_SET;
                    }
                    continueRead = false;
                }
                break;
                case READ_MULTI_STMT_DISABLE_RESULT: {
                    readDisableMultiStmtResponse(cumulateBuffer, serverStatusConsumer);
                    taskEnd = true;
                    continueRead = false;
                }
                break;
                case LOCAL_INFILE_REQUEST: {
                    throw new IllegalStateException(String.format("%s phase[%s] error.", this, this.phase));
                }
                default:
                    throw MySQLExceptions.createUnknownEnumException(this.phase);
            }
        }
        if (taskEnd) {
            if (this.mode == Mode.TEMP_MULTI && this.tempMultiStmtStatus == TempMultiStmtStatus.ENABLE_SUCCESS) {
                taskEnd = false;
                this.phase = Phase.READ_MULTI_STMT_DISABLE_RESULT;
                this.packetPublisher = Mono.just(createSetOptionPacket(false));
            }
        }
        if (taskEnd) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("COM_QUERY instant[{}] task end.", this.hashCode());
            }
            this.phase = Phase.TASK_EN;
            if (hasError()) {
                this.downstreamSink.error(createException());
            } else {
                this.downstreamSink.complete();
            }
        }
        return taskEnd;
    }

    @Override
    protected Action internalError(Throwable e) {
        if (this.phase == Phase.TASK_EN) {
            LOG.error("Unknown error.", e);
        } else {
            this.phase = Phase.TASK_EN;
            addError(MySQLExceptions.wrap(e));
            this.downstreamSink.error(createException());
        }
        return Action.TASK_END;
    }


    /*################################## blow private method ##################################*/

    /**
     * @return true: task end.
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readEnableMultiStmtResponse(final ByteBuf cumulateBuffer
            , final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_MULTI_STMT_ENABLE_RESULT);

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        cumulateBuffer.skipBytes(1); // skip sequence id

        final int status = PacketUtils.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex());
        boolean taskEnd;
        switch (status) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("COM_SET_OPTION enable failure,{}", error);
                }
                // release ByteBuf
                Flux.from(Objects.requireNonNull(this.packetPublisher, "this.packetPublisher"))
                        .map(ByteBuf::release)
                        .subscribe();
                this.packetPublisher = null;
                this.tempMultiStmtStatus = TempMultiStmtStatus.ENABLE_FAILURE;
                addError(MySQLExceptions.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case EofPacket.EOF_HEADER:
            case OkPacket.OK_HEADER: {
                OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok.getStatusFags());
                this.tempMultiStmtStatus = TempMultiStmtStatus.ENABLE_SUCCESS;
                taskEnd = false;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("COM_SET_OPTION enable success.");
                }
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("COM_SET_OPTION response status[%s] error.", status);
        }
        return taskEnd;
    }


    /**
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private void readDisableMultiStmtResponse(final ByteBuf cumulateBuffer
            , final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_MULTI_STMT_DISABLE_RESULT);

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        cumulateBuffer.skipBytes(1); // skip sequence_id

        final int status = PacketUtils.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex());
        switch (status) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("COM_SET_OPTION disabled failure,{}", error);
                }
                this.tempMultiStmtStatus = TempMultiStmtStatus.DISABLE_FAILURE;
                addError(MySQLExceptions.createErrorPacketException(error));
            }
            break;
            case EofPacket.EOF_HEADER:
            case OkPacket.OK_HEADER: {
                OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok.getStatusFags());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("COM_SET_OPTION disabled success.");
                }
                this.tempMultiStmtStatus = TempMultiStmtStatus.DISABLE_SUCCESS;
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("COM_SET_OPTION response status[%s] error.", status);
        }
    }

    /**
     * @return true: task end.
     * @see #internalDecode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    private boolean readResponseResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_RESPONSE_RESULT_SET);

        final ComQueryResponse response = detectComQueryResponseType(cumulateBuffer, this.negotiatedCapability);
        boolean taskEnd = false;
        switch (response) {
            case ERROR: {
                final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer)); //  sequence_id
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                addErrorForSqlError(error);
                taskEnd = true;
            }
            break;
            case OK: {
                final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));
                OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);

                serverStatusConsumer.accept(ok.getStatusFags());
                final ResultStates resultStates = MySQLResultStates.from(ok);

                final int resultSequenceId = this.currentResultSequenceId;
                updateLastResultStates(resultSequenceId, resultStates);
                // emit update result.
                if (this.downstreamSink.skipRestResults()) {
                    addMultiStatementException();
                } else {
                    taskEnd = this.downstreamSink.nextUpdate(resultSequenceId, resultStates);
                }
                if (!taskEnd) {
                    if (this.mode == Mode.SINGLE_STMT) {
                        taskEnd = resultSequenceId == this.sqlCount || hasError();
                    } else {
                        taskEnd = !resultStates.hasMoreResults();
                    }
                }
            }
            break;
            case LOCAL_INFILE_REQUEST: {
                this.phase = Phase.LOCAL_INFILE_REQUEST;
                sendLocalFile(cumulateBuffer);
                this.phase = Phase.READ_RESPONSE_RESULT_SET;
            }
            break;
            case TEXT_RESULT: {
                this.phase = Phase.READ_TEXT_RESULT_SET;
                taskEnd = readTextResultSet(cumulateBuffer, serverStatusConsumer);
            }
            break;
            default:
                throw MySQLExceptions.createUnknownEnumException(response);
        }
        return taskEnd;
    }


    /**
     * @return true: task end.
     */
    private boolean readTextResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_TEXT_RESULT_SET);

        final int resultSequenceId = this.currentResultSequenceId;
        final boolean resultSetEnd;
        if (this.dirtyResultSetReader != null || this.downstreamSink.skipRestResults()) {
            addMultiStatementException();
            resultSetEnd = skipTextResultSet(cumulateBuffer, serverStatusConsumer);
        } else {
            resultSetEnd = this.downstreamSink.readTextResultSet(resultSequenceId, cumulateBuffer
                    , serverStatusConsumer);
        }
        final boolean taskEnd;
        if (resultSetEnd) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Text ResultSet[resultSequenceId={}] end.", resultSequenceId);
            }
            this.phase = Phase.READ_RESPONSE_RESULT_SET;
            if (this.mode == Mode.SINGLE_STMT) {
                taskEnd = resultSequenceId == this.sqlCount || hasError();
            } else {
                taskEnd = !hasMoreResults();
            }
        } else {
            taskEnd = false;
        }
        return taskEnd;
    }

    /**
     * @see #internalStart(TaskSignal)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_set_option.html">Protocol::COM_SET_OPTION</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a53f60000da139fc7d547db96635a2c02">enum_mysql_set_option</a>
     */
    private ByteBuf createSetOptionPacket(final boolean enable) {
        ByteBuf packet = this.adjutant.allocator().buffer(7);
        PacketUtils.writeInt3(packet, 3);
        packet.writeByte(0);//use 0 sequenceId

        packet.writeByte(PacketUtils.COM_SET_OPTION);
        //MYSQL_OPTION_MULTI_STATEMENTS_ON : 0
        //MYSQL_OPTION_MULTI_STATEMENTS_OFF : 1
        PacketUtils.writeInt2(packet, enable ? 0 : 1);

        return packet;
    }


    private void addError(JdbdException e) {
        //TODO filter same error.
        List<JdbdException> errorList = this.errorList;
        if (errorList == null) {
            errorList = new ArrayList<>();
            this.errorList = errorList;
        }
        errorList.add(e);
    }

    private JdbdException createException() {
        List<JdbdException> errorList = this.errorList;
        if (MySQLCollections.isEmpty(errorList)) {
            throw new IllegalStateException(String.format("%s No error,reject creat exception.", this));
        }
        JdbdException e;
        if (errorList.size() == 1) {
            e = errorList.get(0);
        } else {
            e = new JdbdCompositeException(errorList, "occur multi error");
        }
        return e;
    }

    /**
     * @see #readResponseResultSet(ByteBuf, Consumer)
     */
    private void addErrorForSqlError(ErrorPacket error) {
        List<JdbdException> errorList = this.errorList;
        JdbdException e = null;
        if (errorList != null && errorList.size() == 1) {
            JdbdException first = errorList.get(0);
            if (first instanceof LocalFileException) {
                SQLException sqlError = new SQLException(error.getErrorMessage()
                        , error.getSqlState(), error.getErrorCode(), first);
                LocalFileException fileError = (LocalFileException) first;
                e = new JdbdSQLException(sqlError, "Local file[%s] send failure,have sent %s bytes."
                        , fileError.getLocalFile(), fileError.getSentBytes());

                errorList.remove(0);
            }
        }
        if (e == null) {
            e = MySQLExceptions.createErrorPacketException(error);
        }
        addError(e);
    }

    /**
     * @see #readResponseResultSet(ByteBuf, Consumer)
     */
    private void updateLastResultStates(final int resultSequenceId, final ResultStates resultStates) {
        final int currentSequenceId = this.currentResultSequenceId;
        if (currentSequenceId > this.sqlCount) {
            throw new IllegalStateException(String.format("sqlCount[%s] but currentResultSequenceId[%s],state error."
                    , this.sqlCount, currentSequenceId));
        }
        if (resultSequenceId != currentSequenceId) {
            throw new IllegalArgumentException(
                    String.format("currentResultSequenceId[%s] and resultSequenceId[%s] not match."
                            , currentSequenceId, resultSequenceId));
        }

        Pair<Integer, ResultStates> pair = this.lastResultStates;
        if (pair != null && pair.getFirst() != resultSequenceId - 1) {
            throw new IllegalStateException(String.format(
                    "%s lastResultStates[sequenceId:%s] but expect update to sequenceId:%s ."
                    , this, pair.getFirst(), resultSequenceId));
        }
        this.lastResultStates = new Pair<>(resultSequenceId, resultStates);
        this.currentResultSequenceId++;
    }

    private boolean hasError() {
        return !MySQLCollections.isEmpty(this.errorList);
    }

    private boolean hasException(Class<? extends JdbdException> clazz) {
        List<JdbdException> errorList = this.errorList;
        if (errorList != null) {
            for (JdbdException e : errorList) {
                if (clazz.isInstance(e)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void addMultiStatementException() {
        if (!hasError()) {
            addError(MySQLExceptions.createMultiStatementException());
        }
    }

    private boolean hasMoreResults() {
        Pair<Integer, ResultStates> pair = this.lastResultStates;
        return pair != null && pair.getSecond().hasMoreResults();
    }


    /**
     * @see #readResponseResultSet(ByteBuf, Consumer)
     */
    private void sendLocalFile(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.LOCAL_INFILE_REQUEST);

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));
        if (PacketUtils.readInt1AsInt(cumulateBuffer) != PacketUtils.LOCAL_INFILE) {
            throw new IllegalStateException(String.format("%s invoke sendLocalFile method error.", this));
        }
        String localFilePath;
        localFilePath = PacketUtils.readStringFixed(cumulateBuffer, payloadLength - 1
                , this.adjutant.obtainCharsetClient());

        final Path filePath = Paths.get(localFilePath);

        Publisher<ByteBuf> publisher = null;
        if (Files.exists(filePath)) {
            if (Files.isDirectory(filePath)) {
                addError(new LocalFileException(filePath, "Local file[%s] isn directory.", filePath));
            } else if (Files.isReadable(filePath)) {
                try {
                    if (Files.size(filePath) > 0L) {
                        publisher = Flux.create(sink -> doSendLocalFile(sink, filePath));
                    }
                } catch (IOException e) {
                    addError(new LocalFileException(e, filePath, 0L, "Local file[%s] isn't readable.", filePath));
                }

            } else {
                addError(new LocalFileException(filePath, "Local file[%s] isn't readable.", filePath));
            }
        } else {
            addError(new LocalFileException(filePath, "Local file[%s] not exits.", filePath));

        }
        if (publisher == null) {
            publisher = Mono.just(createEmptyPacket());
        }
        this.packetPublisher = publisher;
    }


    /**
     * @see #sendLocalFile(ByteBuf)
     */
    private void doSendLocalFile(final FluxSink<ByteBuf> sink, final Path localPath) {
        long sentBytes = 0L;
        ByteBuf packet = null;
        try (Reader reader = Files.newBufferedReader(localPath, StandardCharsets.UTF_8)) {
            final Charset clientCharset = this.adjutant.obtainCharsetClient();
            final CharBuffer charBuffer = CharBuffer.allocate(1024);
            ByteBuffer byteBuffer;

            // use single packet send local file.
            final int maxPacket = PacketUtils.MAX_PACKET - 1;

            packet = this.adjutant.createPacketBuffer(2048);
            while (reader.read(charBuffer) > 0) { // 1. read chars
                byteBuffer = clientCharset.encode(charBuffer); // 2.encode
                packet.writeBytes(byteBuffer);                // 3. write bytes
                charBuffer.clear();                           // 4. clear char buffer.

                //5. send single packet(not multi packet).
                if (packet.readableBytes() >= maxPacket) {
                    ByteBuf tempPacket = packet.readRetainedSlice(maxPacket);
                    PacketUtils.writePacketHeader(tempPacket, addAndGetSequenceId());
                    sink.next(tempPacket);
                    sentBytes += (maxPacket - PacketUtils.HEADER_SIZE);

                    tempPacket = this.adjutant.createPacketBuffer(Math.max(2048, packet.readableBytes()));
                    tempPacket.writeBytes(packet);
                    packet.release();
                    packet = tempPacket;
                }
            }

            if (packet.readableBytes() == PacketUtils.HEADER_SIZE) {
                sink.next(packet); // send empty packet, tell server file end.
            } else {
                PacketUtils.writePacketHeader(packet, addAndGetSequenceId());
                sink.next(packet);
                sentBytes += (packet.readableBytes() - PacketUtils.HEADER_SIZE);

                sink.next(createEmptyPacket());
            }
        } catch (Throwable e) {
            if (packet != null) {
                packet.release();
            }
            addError(new LocalFileException(e, localPath, sentBytes, "Local file[%s] send failure,sent %s bytes."
                    , localPath, sentBytes));
            sink.next(createEmptyPacket());
        } finally {
            sink.complete();
        }
    }

    /**
     * @see #sendLocalFile(ByteBuf)
     * @see #doSendLocalFile(FluxSink, Path)
     */
    private ByteBuf createEmptyPacket() {
        ByteBuf packet = this.adjutant.allocator().buffer(PacketUtils.HEADER_SIZE);
        PacketUtils.writeInt3(packet, 0);
        packet.writeByte(addAndGetSequenceId());
        return packet;
    }

    /**
     * @see SingleStatementBatchUpdate#nextUpdate(int, ResultStates)
     */
    private void sendStaticCommand(final String sql) throws SQLException {
        // result sequence_id
        this.updateSequenceId(-1);
        this.packetPublisher = ComQueryCommandWriter.createStaticSingleCommand(sql, this::addAndGetSequenceId
                , this.adjutant);
    }

    /**
     * @see BindableSingleStatementBatchUpdate#nextUpdate(int, ResultStates)
     */
    private void sendBindableCommand(final String sql, final List<BindValue> paramGroup) throws SQLException {
        // result sequence_id
        this.updateSequenceId(-1);
        BindableWrapper bindableWrapper = StmtWrappers.multi(sql, paramGroup);
        this.packetPublisher = Flux.fromIterable(
                ComQueryCommandWriter.createBindableCommand(bindableWrapper, this::addAndGetSequenceId, this.adjutant)
        );
    }

    /**
     * @return true result set end.
     */
    private boolean skipTextResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        ResultSetReader dirtyResultSetReader = this.dirtyResultSetReader;
        if (dirtyResultSetReader == null) {
            // create a resettable Reader
            dirtyResultSetReader = createResettableDirtyResultReader();
            this.dirtyResultSetReader = dirtyResultSetReader;
        }
        return dirtyResultSetReader.read(cumulateBuffer, serverStatusConsumer);
    }

    private ResultSetReader createResettableDirtyResultReader() {
        return ResultSetReaderBuilder.builder()
                .rowSink(createDirtyRowSink())
                .adjutant(ComQueryTask.this.adjutant)
                .sequenceIdUpdater(ComQueryTask.this::updateSequenceId)

                .errorConsumer(ComQueryTask.this::addError)
                .resettable(true)
                .build(TextResultSetReader.class);
    }

    private ResultRowSink createDirtyRowSink() {
        return new ResultRowSink() {
            @Override
            public void next(ResultRow resultRow) {
                //no-op
            }

            @Override
            public boolean isCancelled() {
                return true;
            }

            @Override
            public void accept(ResultStates resultStates) {
                ComQueryTask.this.updateLastResultStates(ComQueryTask.this.currentResultSequenceId, resultStates);
            }
        };
    }


    private void assertPhase(Phase expect) {
        if (this.phase != expect) {
            throw new IllegalStateException(String.format("%s current phase isn't %s .", this, expect));
        }
    }


    private void assertSingleMode(DownstreamSink sink) {
        if (this.mode != Mode.SINGLE_STMT) {
            throw new IllegalStateException(String.format("Mode[%s] isn't %s,reject create %s instance."
                    , this.mode, Mode.SINGLE_STMT, sink));
        }
    }


    /*################################## blow private instance class ##################################*/

    private interface DownstreamSink {

        void error(JdbdException e);

        /**
         * @param resultSequenceId base 1.
         * @return true:create next update COM_QUERY packet occur error,task end.
         */
        boolean nextUpdate(int resultSequenceId, ResultStates resultStates);

        /**
         * @param resultSequenceId base 1.
         * @return true: text result end.
         */
        boolean readTextResultSet(int resultSequenceId, ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);

        void complete();

        boolean skipRestResults();


    }


    private final class SingleQuerySink implements DownstreamSink, ResultRowSink {

        private final FluxSink<ResultRow> sink;

        private final Consumer<ResultStates> statesConsumer;

        private final ResultSetReader resultSetReader;

        private ResultStates resultStates;

        private boolean resultEnd;

        /**
         * @see #ComQueryTask(String, FluxSink, Consumer, MySQLTaskAdjutant)
         * @see #ComQueryTask(BindableWrapper, FluxSink, MySQLTaskAdjutant)
         */
        private SingleQuerySink(FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer) {
            assertSingleMode(this);

            this.sink = sink;
            this.statesConsumer = statesConsumer;
            this.resultSetReader = ResultSetReaderBuilder.builder()
                    .rowSink(this)
                    .adjutant(ComQueryTask.this.adjutant)
                    .sequenceIdUpdater(ComQueryTask.this::updateSequenceId)
                    .errorConsumer(ComQueryTask.this::addError)

                    .resettable(false)
                    .build(TextResultSetReader.class);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }

        @Override
        public void error(final JdbdException e) {
            this.sink.error(e);
        }

        /**
         * @see ResultRowSink#next(ResultRow)
         */
        @Override
        public void next(ResultRow resultRow) {
            this.sink.next(resultRow);
        }

        /**
         * @see ResultRowSink#isCancelled()
         */
        @Override
        public boolean isCancelled() {
            return ComQueryTask.this.hasError() || this.sink.isCancelled();
        }

        /**
         * @see ResultRowSink#accept(ResultStates)
         */
        @Override
        public void accept(ResultStates resultStates) {
            if (this.resultStates != null) {
                throw new IllegalStateException(String.format("%s.resultStates isn't null,reject update.", this));
            }
            this.resultStates = resultStates;
            ComQueryTask.this.updateLastResultStates(1, resultStates);
        }

        @Override
        public boolean nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            if (resultSequenceId == 1) {
                addError(new ErrorSubscribeException(ResultType.QUERY, ResultType.UPDATE));
            } else {
                addMultiStatementException();
            }
            this.resultEnd = true;
            return false;
        }

        @Override
        public boolean readTextResultSet(int resultSequenceId, final ByteBuf cumulateBuffer
                , final Consumer<Object> serverStatusConsumer) {
            final boolean resultEnd;
            if (resultSequenceId == 1) {
                resultEnd = this.resultSetReader.read(cumulateBuffer, serverStatusConsumer);
                if (resultEnd && this.resultStates == null) {
                    throw new IllegalStateException(String.format("%s, %s not invoke ResultStates Consumer."
                            , this, this.resultSetReader.getClass().getName()));
                }
            } else {
                addMultiStatementException();
                resultEnd = ComQueryTask.this.skipTextResultSet(cumulateBuffer, serverStatusConsumer);
            }
            if (resultEnd) {
                this.resultEnd = true;
            }
            return resultEnd;
        }

        @Override
        public void complete() {
            if (this.sink.isCancelled()) {
                return;
            }
            try {
                // invoke user ResultStates Consumer.
                this.statesConsumer.accept(Objects.requireNonNull(this.resultStates, "this.resultStates"));
                this.sink.complete();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{} complete.", this.getClass().getSimpleName());
                }
            } catch (Throwable e) {
                this.sink.error(new ResultStateConsumerException(e, "%s consumer error."
                        , ResultStates.class.getName()));
            }
        }

        @Override
        public boolean skipRestResults() {
            return this.resultEnd;
        }
    }

    private final class SingleUpdateSink implements DownstreamSink {

        private final MonoSink<ResultStates> sink;

        private ResultStates resultStates;

        private boolean resultEnd;

        /**
         * @see #ComQueryTask(String, MonoSink, MySQLTaskAdjutant)
         * @see #ComQueryTask(BindableWrapper, MonoSink, MySQLTaskAdjutant)
         */
        private SingleUpdateSink(MonoSink<ResultStates> sink) {
            assertSingleMode(this);
            this.sink = sink;
        }

        @Override
        public void error(final JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public void complete() {
            this.sink.success(Objects.requireNonNull(this.resultStates, "this.resultStates"));
        }

        @Override
        public boolean nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            if (resultSequenceId == 1) {
                this.resultStates = resultStates;
            } else {
                addMultiStatementException();
            }
            this.resultEnd = true;
            return false;
        }

        @Override
        public boolean readTextResultSet(final int resultSequenceId, final ByteBuf cumulateBuffer
                , final Consumer<Object> serverStatusConsumer) {
            if (resultSequenceId == 1) {
                addError(new ErrorSubscribeException(ResultType.UPDATE, ResultType.QUERY));
            } else {
                addMultiStatementException();
            }
            boolean resultEnd;
            resultEnd = ComQueryTask.this.skipTextResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultEnd) {
                this.resultEnd = true;
            }
            return resultEnd;
        }

        @Override
        public boolean skipRestResults() {
            return this.resultEnd;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    private final class SingleStatementBatchUpdate implements DownstreamSink {

        private final List<String> sqlList;

        private final FluxSink<ResultStates> sink;

        private int currentSequenceId = 1;

        /**
         * @see #ComQueryTask(List, FluxSink, MySQLTaskAdjutant)
         */
        private SingleStatementBatchUpdate(List<String> sqlList, FluxSink<ResultStates> sink) {
            this.sqlList = sqlList;
            this.sink = sink;
            if (sqlList.size() != ComQueryTask.this.sqlCount) {
                throw new IllegalArgumentException(String.format("sqlList size[%s] ans sqlCount[%s] not match."
                        , sqlList.size(), ComQueryTask.this.sqlCount));
            }
        }

        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public boolean nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            final int batchCount = ComQueryTask.this.sqlCount;
            boolean taskEnd = false;
            if (resultSequenceId > batchCount) {
                addMultiStatementException();
                taskEnd = true;
            } else if (ComQueryTask.this.hasError()) {
                taskEnd = true;
            } else if (this.currentSequenceId == resultSequenceId) {
                this.sink.next(resultStates);
                try {
                    int index = this.currentSequenceId++;
                    if (index < batchCount) {
                        ComQueryTask.this.sendStaticCommand(this.sqlList.get(index));
                    }
                } catch (Throwable e) {
                    ComQueryTask.this.addError(MySQLExceptions.wrap(e));
                    taskEnd = true;
                }
            } else if (!hasError()) {
                throw new IllegalStateException(String.format(
                        "resultSequenceId[%s] and this.currentSequenceId[%s] not match."
                        , resultSequenceId, this.currentSequenceId));
            }
            return taskEnd;
        }

        @Override
        public boolean readTextResultSet(final int resultSequenceId, ByteBuf cumulateBuffer
                , Consumer<Object> serverStatusConsumer) {
            if (!hasException(ErrorSubscribeException.class)) {
                addError(new ErrorSubscribeException(ResultType.BATCH_UPDATE, ResultType.QUERY));
            }
            boolean resultEnd;
            resultEnd = ComQueryTask.this.skipTextResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultEnd) {
                this.currentSequenceId = ComQueryTask.this.sqlCount + 1;
            }
            LOG.debug("temp multi stmt error :{} , {}", resultSequenceId, resultEnd);
            return resultEnd;
        }

        @Override
        public void complete() {
            this.sink.complete();
        }

        @Override
        public boolean skipRestResults() {
            return this.currentSequenceId > ComQueryTask.this.sqlCount;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    private final class BindableSingleStatementBatchUpdate implements DownstreamSink {

        private final String sql;

        private final List<List<BindValue>> parameterGroupList;

        private final FluxSink<ResultStates> sink;

        private int currentSequenceId = 1;

        /**
         * @see #ComQueryTask(BatchBindWrapper, FluxSink, MySQLTaskAdjutant)
         */
        private BindableSingleStatementBatchUpdate(final BatchBindWrapper wrapper, FluxSink<ResultStates> sink) {
            this.sql = wrapper.getSql();
            this.parameterGroupList = wrapper.getParamGroupList();
            this.sink = sink;
            if (parameterGroupList.size() != ComQueryTask.this.sqlCount) {
                String message = String.format("parameterGroupList size[%s] ans sqlCount[%s] not match."
                        , parameterGroupList.size(), ComQueryTask.this.sqlCount);
                throw new IllegalArgumentException(message);
            }
        }

        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public boolean nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            final int batchCount = ComQueryTask.this.sqlCount;
            boolean taskEnd = false;
            if (resultSequenceId > batchCount) {
                addMultiStatementException();
                taskEnd = true;
            } else if (ComQueryTask.this.hasError()) {
                taskEnd = true;
            } else if (this.currentSequenceId == resultSequenceId) {
                this.sink.next(resultStates);
                final int index = this.currentSequenceId++;
                try {
                    if (index < batchCount) {
                        ComQueryTask.this.sendBindableCommand(this.sql, this.parameterGroupList.get(index));
                    }
                } catch (Throwable e) {
                    ComQueryTask.this.addError(MySQLExceptions.wrap(e, "paramGroup[%s]", index));
                    taskEnd = true;
                }
            } else if (!hasError()) {
                throw new IllegalStateException(String.format(
                        "resultSequenceId[%s] and this.currentSequenceId[%s] not match."
                        , resultSequenceId, this.currentSequenceId));
            }
            return taskEnd;
        }

        @Override
        public boolean readTextResultSet(final int resultSequenceId, ByteBuf cumulateBuffer
                , Consumer<Object> serverStatusConsumer) {
            if (!hasException(ErrorSubscribeException.class)) {
                addError(new ErrorSubscribeException(ResultType.BATCH_UPDATE, ResultType.QUERY));
            }
            boolean resultEnd;
            resultEnd = ComQueryTask.this.skipTextResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultEnd) {
                this.currentSequenceId = ComQueryTask.this.sqlCount + 1;
            }
            LOG.debug("temp multi stmt error :{} , {}", resultSequenceId, resultEnd);
            return resultEnd;
        }

        @Override
        public void complete() {
            this.sink.complete();
        }

        @Override
        public boolean skipRestResults() {
            return this.currentSequenceId > ComQueryTask.this.sqlCount;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    private final class MultiStatementBatchUpdateSink implements DownstreamSink {

        private final FluxSink<ResultStates> sink;

        private int currentSequenceId = 1;

        /**
         * @see #ComQueryTask(BatchBindWrapper, FluxSink, MySQLTaskAdjutant)
         */
        private MultiStatementBatchUpdateSink(FluxSink<ResultStates> sink) {
            if (ComQueryTask.this.mode == Mode.SINGLE_STMT) {
                throw new IllegalStateException(String.format("mode[%s] error.", ComQueryTask.this.mode));
            }
            this.sink = sink;
        }

        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public boolean nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            if (resultSequenceId == this.currentSequenceId) {
                if (!ComQueryTask.this.hasError()) {
                    this.sink.next(resultStates);
                }
                this.currentSequenceId++;
            } else if (!hasError()) {
                throw new IllegalStateException(String.format(
                        "resultSequenceId[%s] and this.currentSequenceId[%s] not match."
                        , resultSequenceId, this.currentSequenceId));
            }
            return false;
        }

        @Override
        public boolean readTextResultSet(int resultSequenceId, ByteBuf cumulateBuffer
                , Consumer<Object> serverStatusConsumer) {
            if (!hasException(ErrorSubscribeException.class)) {
                addError(new ErrorSubscribeException(ResultType.BATCH_UPDATE, ResultType.QUERY));
            }
            boolean resultEnd;
            resultEnd = skipTextResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultEnd) {
                this.currentSequenceId = ComQueryTask.this.sqlCount + 1;
            }
            return resultEnd;
        }

        @Override
        public void complete() {
            if (this.currentSequenceId <= ComQueryTask.this.sqlCount) {
                throw new IllegalStateException(String.format("currentSequenceId[%s] error.", this.currentSequenceId));
            }
            this.sink.complete();
        }

        @Override
        public boolean skipRestResults() {
            return this.currentSequenceId > ComQueryTask.this.sqlCount;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }


    private final class MultiStmtSink implements DownstreamSink, ResultRowSink {

        private final MultiResultsSink sink;

        private final ResultSetReader resultSetReader;

        private QuerySink querySink;

        private int currentSequenceId = 1;

        /**
         * @see #ComQueryTask(List, MultiResultsSink, MySQLTaskAdjutant)
         * @see #ComQueryTask(MultiResultsSink, List, MySQLTaskAdjutant)
         */
        private MultiStmtSink(MultiResultsSink sink) {
            if (ComQueryTask.this.mode != Mode.MULTI_STMT) {
                throw new IllegalStateException(String.format("%s mode[%s] error,reject create instance."
                        , this, ComQueryTask.this.mode));
            }
            this.sink = sink;
            this.resultSetReader = ResultSetReaderBuilder.builder()
                    .rowSink(this)
                    .adjutant(ComQueryTask.this.adjutant)
                    .sequenceIdUpdater(ComQueryTask.this::updateSequenceId)
                    .errorConsumer(ComQueryTask.this::addError)

                    .resettable(true)
                    .build(TextResultSetReader.class);
        }

        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public void complete() {
            if (this.currentSequenceId <= ComQueryTask.this.sqlCount || ComQueryTask.this.hasMoreResults()) {
                // hear bug.
                throw new IllegalStateException(String.format(
                        "%s has more results,current sequenceId[%s],expect result count[%s], reject complete."
                        , this, this.currentSequenceId, ComQueryTask.this.sqlCount));
            }
        }

        /**
         * @see ResultRowSink#next(ResultRow)
         */
        @Override
        public void next(ResultRow resultRow) {
            this.querySink.next(resultRow);
        }

        /**
         * @see ResultRowSink#isCancelled()
         */
        @Override
        public boolean isCancelled() {
            return this.querySink.isCancelled();
        }

        /**
         * @see ResultRowSink#accept(ResultStates)
         */
        @Override
        public void accept(ResultStates resultStates) throws IllegalStateException {
            updateLastResultStates(this.currentSequenceId, resultStates);
            if (!ComQueryTask.this.hasError()) {
                this.querySink.accept(resultStates);
            }
        }

        @Override
        public boolean nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            if (resultSequenceId == this.currentSequenceId) {
                if (!ComQueryTask.this.hasError()) {
                    this.sink.nextUpdate(resultStates);
                }
                this.currentSequenceId++;
                if (resultSequenceId == ComQueryTask.this.sqlCount && resultStates.hasMoreResults()) {
                    throw new IllegalStateException(String.format(
                            "%s has more results,current sequenceId[%s],expect result count[%s]."
                            , this, this.currentSequenceId, ComQueryTask.this.sqlCount));
                }
            } else if (!hasError()) {
                throw new IllegalStateException(String.format(
                        "resultSequenceId[%s] and this.currentSequenceId[%s] not match."
                        , resultSequenceId, this.currentSequenceId));
            }
            return false;
        }

        @Override
        public boolean readTextResultSet(final int resultSequenceId, ByteBuf cumulateBuffer
                , final Consumer<Object> serverStatusConsumer) {
            final boolean resultEnd;
            if (resultSequenceId == this.currentSequenceId) {
                if (this.querySink == null) {
                    this.querySink = this.sink.nextQuery();
                }
                resultEnd = this.resultSetReader.read(cumulateBuffer, serverStatusConsumer);
                if (resultEnd) {
                    if (!ComQueryTask.this.hasError()) {
                        this.querySink.complete();
                    }
                    this.querySink = null;
                    this.currentSequenceId++;
                }

            } else if (!hasError()) {
                throw new IllegalStateException(String.format(
                        "resultSequenceId[%s] and this.currentSequenceId[%s] not match."
                        , resultSequenceId, this.currentSequenceId));
            } else {
                resultEnd = ComQueryTask.this.skipTextResultSet(cumulateBuffer, serverStatusConsumer);
            }
            return resultEnd;
        }

        @Override
        public boolean skipRestResults() {
            return this.currentSequenceId > ComQueryTask.this.sqlCount;
        }
    }


    /**
     * invoke this method after invoke {@link PacketUtils#hasOnePacket(ByteBuf)}.
     *
     * @see #internalDecode(ByteBuf, Consumer)
     */
    static ComQueryResponse detectComQueryResponseType(final ByteBuf cumulateBuffer, final int negotiatedCapability) {
        int readerIndex = cumulateBuffer.readerIndex();
        final int payloadLength = PacketUtils.getInt3(cumulateBuffer, readerIndex);
        // skip header
        readerIndex += PacketUtils.HEADER_SIZE;
        ComQueryResponse responseType;
        final boolean metadata = (negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;

        switch (PacketUtils.getInt1AsInt(cumulateBuffer, readerIndex++)) {
            case 0: {
                if (metadata && PacketUtils.obtainLenEncIntByteCount(cumulateBuffer, readerIndex) + 1 == payloadLength) {
                    responseType = ComQueryResponse.TEXT_RESULT;
                } else {
                    responseType = ComQueryResponse.OK;
                }
            }
            break;
            case ErrorPacket.ERROR_HEADER:
                responseType = ComQueryResponse.ERROR;
                break;
            case PacketUtils.LOCAL_INFILE:
                responseType = ComQueryResponse.LOCAL_INFILE_REQUEST;
                break;
            default:
                responseType = ComQueryResponse.TEXT_RESULT;

        }
        return responseType;
    }

    /*################################## blow private static method ##################################*/


    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    private enum ComQueryResponse {
        OK,
        ERROR,
        TEXT_RESULT,
        LOCAL_INFILE_REQUEST
    }


    private enum Phase {
        READ_RESPONSE_RESULT_SET,
        READ_TEXT_RESULT_SET,
        LOCAL_INFILE_REQUEST,
        READ_MULTI_STMT_ENABLE_RESULT,
        READ_MULTI_STMT_DISABLE_RESULT,
        TASK_EN
    }


    private enum Mode {
        SINGLE_STMT,
        MULTI_STMT,
        TEMP_MULTI
    }

    private enum TempMultiStmtStatus {
        ENABLE_SUCCESS,
        ENABLE_FAILURE,
        DISABLE_SUCCESS,
        DISABLE_FAILURE
    }


}
