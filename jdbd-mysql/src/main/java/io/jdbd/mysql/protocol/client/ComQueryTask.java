package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.ResultStatusConsumerException;
import io.jdbd.mysql.stmt.*;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStreams;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.*;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.result.*;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import io.jdbd.vendor.stmt.Stmt;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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

    /*################################## blow StaticStatement underlying api method ##################################*/

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     *
     * @see #ComQueryTask(Stmt, FluxResultSink, TaskAdjutant)
     * @see ClientCommandProtocol#update(StaticStmt)
     */
    static Mono<ResultStates> update(final StaticStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.update(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of below methods:
     * <ul>
     *     <li>{@link StaticStatement#executeQuery(String)}</li>
     *     <li>{@link StaticStatement#executeQuery(String, Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see #ComQueryTask(Stmt, FluxResultSink, TaskAdjutant)
     * @see ClientCommandProtocol#query(StaticStmt)
     */
    static Flux<ResultRow> query(final StaticStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatch(List)} method.
     * </p>
     *
     * @see #ComQueryTask(Stmt, FluxResultSink, TaskAdjutant)
     * @see ClientCommandProtocol#batchUpdate(List)
     */
    static Flux<ResultStates> batchUpdate(final StaticBatchStmt stmt, final TaskAdjutant adjutant) {
        final Flux<ResultStates> flux;
        if (stmt.getSqlGroup().isEmpty()) {
            flux = Flux.error(MySQLExceptions.createEmptySqlException());
        } else {
            flux = MultiResults.batchUpdate(sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
                }

            });
        }
        return flux;
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsMulti(List)} method.
     * </p>
     *
     * @see ClientCommandProtocol#executeAsMulti(List)
     * @see #ComQueryTask(Stmt, FluxResultSink, TaskAdjutant)
     */
    static MultiResult batchAsMulti(final StaticBatchStmt stmt, final TaskAdjutant adjutant) {
        final MultiResult result;
        if (stmt.getSqlGroup().isEmpty()) {
            result = MultiResults.error(MySQLExceptions.createEmptySqlException());
        } else {
            result = MultiResults.asMulti(adjutant, sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
                }
            });
        }
        return result;
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(List)} method.
     * </p>
     *
     * @see ClientCommandProtocol#executeAsFlux(List)
     * @see #ComQueryTask(Stmt, FluxResultSink, TaskAdjutant)
     */
    static OrderedFlux batchAsFlux(final StaticBatchStmt stmt, final TaskAdjutant adjutant) {
        final OrderedFlux flux;
        if (stmt.getSqlGroup().isEmpty()) {
            flux = MultiResults.orderedFluxError(MySQLExceptions.createEmptySqlException());
        } else {
            flux = MultiResults.asFlux(sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
                }
            });
        }
        return flux;
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(String)} method.
     * </p>
     */
    static OrderedFlux multiCommandAsFlux(StaticStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    /*################################## blow BindableStatement underlying api method ##################################*/

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeUpdate()} method.
     * </p>
     *
     * @see ComPreparedTask#update(ParamStmt, TaskAdjutant)
     * @see ClientCommandProtocol#bindableUpdate(BindStmt)
     */
    static Mono<ResultStates> bindableUpdate(final BindStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.update(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }

        });
    }

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindStatement#executeQuery()}</li>
     *     <li>{@link BindStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see ClientCommandProtocol#bindableQuery(BindStmt)
     */
    static Flux<ResultRow> bindableQuery(final BindStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatch()} method.
     * </p>
     *
     * @see ClientCommandProtocol#bindableBatch(BindBatchStmt)
     */
    static Flux<ResultStates> bindableBatch(final BindBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    /**
     * <p>
     * This method is one of underlying api of below methods {@link BindStatement#executeBatchAsMulti()}.
     * </p>
     */
    static MultiResult bindableAsMulti(final BindBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of below methods {@link BindStatement#executeBatchAsFlux()}.
     * </p>
     */
    static OrderedFlux bindableAsFlux(final BindBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /*################################## blow MultiStatement method ##################################*/


    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsMulti()} method.
     * </p>
     *
     * @see ClientCommandProtocol#multiStmtAsMulti(List)
     */
    static MultiResult multiStmtAsMulti(final BindMultiStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsFlux()} method.
     * </p>
     *
     * @see ClientCommandProtocol#multiStmtAsFlux(List)
     */
    static OrderedFlux multiStmtAsFlux(final BindMultiStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTask.class);

    private final Stmt stmt;

    private final FluxResultSink sink;

    private Phase phase;


    private List<JdbdException> errorList;


    /**
     * <p>
     * This constructor create instance for {@link #update(StaticStmt, TaskAdjutant)}
     * </p>
     * <p>
     * The rule of {@link StaticStatement} underlying api constructor.
     *     <ul>
     *         <li>param 1 : stmt</li>
     *         <li>param 2 : sink</li>
     *         <li>param 3 : adjutant</li>
     *     </ul>
     * </p>
     *
     * @see #update(StaticStmt, TaskAdjutant)
     * @see #query(StaticStmt, TaskAdjutant)
     */
    private ComQueryTask(final Stmt stmt, FluxResultSink sink, TaskAdjutant adjutant) {
        super(adjutant, sink::error);
        this.stmt = stmt;
        this.sink = sink;
    }


    @Override
    public final String toString() {
        return this.getClass().getSimpleName() + "@" + this.hashCode();
    }


    /*################################## blow package template method ##################################*/

    @Override
    protected Publisher<ByteBuf> start() {
        Publisher<ByteBuf> publisher;
        final Stmt stmt = this.stmt;
        final Supplier<Integer> sequenceId = this::addAndGetSequenceId;
        try {
            if (stmt instanceof StaticStmt) {
                publisher = QueryCommandWriter.createStaticCommand((StaticStmt) stmt, sequenceId, this.adjutant);
            } else if (stmt instanceof StaticBatchStmt) {
                publisher = QueryCommandWriter.create
            } else if (stmt instanceof BindStmt) {

            } else if (stmt instanceof BindBatchStmt) {

            } else if (stmt instanceof)
                if (this.mode == Mode.TEMP_MULTI) {
                    this.phase = Phase.READ_MULTI_STMT_ENABLE_RESULT;
                    publisher = Mono.just(createSetOptionPacket(true));
                } else {
                    this.phase = Phase.READ_RESPONSE_RESULT_SET;
                    publisher = Objects.requireNonNull(this.packetPublisher, "this.packetPublisher");
                    this.packetPublisher = null;
                }
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} send COM_QUERY packet with mode[{}],downstream[{}]", this, this.mode, this.downstreamSink);
            }
            return publisher;
        } catch (SQLException e) {
            publisher = null;

        }
        return publisher;
    }


    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        boolean taskEnd = false;
        boolean continueRead = true;
        while (continueRead) {
            switch (this.phase) {
                case READ_RESPONSE_RESULT_SET: {
                    taskEnd = readResponseResultSet(cumulateBuffer, serverStatusConsumer);
                    continueRead = !taskEnd && Packets.hasOnePacket(cumulateBuffer);
                }
                break;
                case READ_TEXT_RESULT_SET: {
                    taskEnd = readTextResultSet(cumulateBuffer, serverStatusConsumer);
                    continueRead = !taskEnd && Packets.hasOnePacket(cumulateBuffer);
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
                    throw MySQLExceptions.createUnexpectedEnumException(this.phase);
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
    protected Action onError(Throwable e) {
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
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean readEnableMultiStmtResponse(final ByteBuf cumulateBuffer
            , final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_MULTI_STMT_ENABLE_RESULT);

        final int payloadLength = Packets.readInt3(cumulateBuffer);
        cumulateBuffer.skipBytes(1); // skip sequence id

        final int status = Packets.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex());
        boolean taskEnd;
        switch (status) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} COM_SET_OPTION enable failure,{}", this, error);
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
                    LOG.debug("{} COM_SET_OPTION enable success.", this);
                }
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("COM_SET_OPTION response status[%s] error.", status);
        }
        return taskEnd;
    }


    /**
     * @see #decode(ByteBuf, Consumer)
     */
    private void readDisableMultiStmtResponse(final ByteBuf cumulateBuffer
            , final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_MULTI_STMT_DISABLE_RESULT);

        final int payloadLength = Packets.readInt3(cumulateBuffer);
        cumulateBuffer.skipBytes(1); // skip sequence_id

        final int status = Packets.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex());
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
     * @see #decode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    private boolean readResponseResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_RESPONSE_RESULT_SET);

        final ComQueryResponse response = detectComQueryResponseType(cumulateBuffer, this.negotiatedCapability);
        boolean taskEnd = false;
        switch (response) {
            case ERROR: {
                final int payloadLength = Packets.readInt3(cumulateBuffer);
                updateSequenceId(Packets.readInt1AsInt(cumulateBuffer)); //  sequence_id
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                addErrorForSqlError(error);
                taskEnd = true;
                // TODO zoro handle LocalFileException
            }
            break;
            case OK: {
                final int payloadLength = Packets.readInt3(cumulateBuffer);
                updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
                OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);

                serverStatusConsumer.accept(ok.getStatusFags());
                // emit update result.
                taskEnd = this.downstreamSink.nextUpdate(MySQLResultStates.from(ok));
                // TODO zoro handle LocalFileException
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
                throw MySQLExceptions.createUnexpectedEnumException(response);
        }
        return taskEnd;
    }

    /**
     * <p>
     * when text result set end, update {@link #phase}.
     * </p>
     *
     * @return true: task end.
     */
    private boolean readTextResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_TEXT_RESULT_SET);
        final boolean taskEnd;
        if (this.downstreamSink.readResultSet(cumulateBuffer, serverStatusConsumer)) {
            if (this.downstreamSink.hasMoreResult()) {
                this.phase = Phase.READ_RESPONSE_RESULT_SET;
                taskEnd = false;
            } else if (hasError()) {
                taskEnd = true;
            } else if (this.downstreamSink instanceof SingleModeBatchDownstreamSink) {
                final SingleModeBatchDownstreamSink sink = (SingleModeBatchDownstreamSink) this.downstreamSink;
                taskEnd = !sink.hasMoreGroup() || sink.sendCommand();
                if (!taskEnd) {
                    this.phase = Phase.READ_RESPONSE_RESULT_SET;
                }
            } else {
                taskEnd = true;
            }
        } else {
            taskEnd = false;
        }
        return taskEnd;
    }


    /**
     * @see #start()
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_set_option.html">Protocol::COM_SET_OPTION</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a53f60000da139fc7d547db96635a2c02">enum_mysql_set_option</a>
     */
    private ByteBuf createSetOptionPacket(final boolean enable) {
        ByteBuf packet = this.adjutant.allocator().buffer(7);
        Packets.writeInt3(packet, 3);
        packet.writeByte(0);//use 0 sequenceId

        packet.writeByte(Packets.COM_SET_OPTION);
        //MYSQL_OPTION_MULTI_STATEMENTS_ON : 0
        //MYSQL_OPTION_MULTI_STATEMENTS_OFF : 1
        Packets.writeInt2(packet, enable ? 0 : 1);

        return packet;
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
            e = new JdbdCompositeException(errorList);
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


    private boolean containException(Class<? extends JdbdException> clazz) {
        List<JdbdException> errorList = this.errorList;
        boolean contain = false;
        if (errorList != null) {
            for (JdbdException e : errorList) {
                if (clazz.isInstance(e)) {
                    contain = true;
                    break;
                }
            }
        }
        return contain;
    }

    private void replaceIfNeed(Function<JdbdException, JdbdException> function) {
        final List<JdbdException> errorList = this.errorList;
        if (errorList != null) {
            final int size = errorList.size();
            JdbdException temp;
            for (int i = 0; i < size; i++) {
                temp = function.apply(errorList.get(i));
                if (temp != null) {
                    errorList.set(i, temp);
                    break;
                }
            }
        }


    }

    /**
     * @see #readResponseResultSet(ByteBuf, Consumer)
     */
    private void sendLocalFile(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.LOCAL_INFILE_REQUEST);

        final int payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
        if (Packets.readInt1AsInt(cumulateBuffer) != Packets.LOCAL_INFILE) {
            throw new IllegalStateException(String.format("%s invoke sendLocalFile method error.", this));
        }
        String localFilePath;
        localFilePath = Packets.readStringFixed(cumulateBuffer, payloadLength - 1
                , this.adjutant.charsetClient());

        final Path filePath = Paths.get(localFilePath);

        Publisher<ByteBuf> publisher = null;

        try {
            if (Files.notExists(filePath, LinkOption.NOFOLLOW_LINKS)) {
                String message = String.format("Local file[%s] not exits.", filePath);
                throw new LocalFileException(filePath, message);
            } else if (Files.isDirectory(filePath)) {
                String message = String.format("Local file[%s] isn directory.", filePath);
                throw new LocalFileException(filePath, message);
            } else if (!Files.isReadable(filePath)) {
                String message = String.format("Local file[%s] isn't readable.", filePath);
                throw new LocalFileException(filePath, message);
            } else {
                try {
                    if (Files.size(filePath) > 0L) {
                        publisher = createLocalFilePacketFlux(filePath);
                    }
                } catch (IOException e) {
                    String message = String.format("Local file[%s] isn't readable.", filePath);
                    throw new LocalFileException(filePath, 0L, message, e);
                }
            }
        } catch (Throwable e) {
            if (e instanceof LocalFileException) {
                addError(MySQLExceptions.wrap(e));
            } else {
                String message = String.format("Local file[%s] read occur error.", filePath);
                addError(new LocalFileException(filePath, message));
            }
        }
        if (publisher == null) {
            publisher = Mono.just(Packets.createEmptyPacket(this.adjutant.allocator(), addAndGetSequenceId()));
        }
        this.packetPublisher = publisher;
    }


    private Flux<ByteBuf> createLocalFilePacketFlux(final Path localPath) {
        return Flux.create(sink -> {

            try {
                if (this.adjutant.inEventLoop()) {
                    doSendLocalFile(sink, localPath);
                } else {
                    synchronized (ComQueryTask.this) {
                        doSendLocalFile(sink, localPath);
                    }
                }
            } catch (Throwable e) {
                if (e instanceof LocalFileException) {
                    sink.error(e);
                } else {
                    String message = String.format("Local file[%s] send occur error.", localPath);
                    sink.error(new LocalFileException(localPath, message));
                }
            }

        });
    }

    /**
     * @see #createLocalFilePacketFlux(Path)
     */
    private void doSendLocalFile(final FluxSink<ByteBuf> sink, final Path localPath) {

        ByteBuf packet = null;
        long sentBytes = 0L;

        try (FileChannel channel = FileChannel.open(localPath, StandardOpenOption.READ)) {
            //1. firstly obtain file encoding
            final Charset fileEncoding = MySQLStreams.fileEncodingOrUtf8();
            final Charset clientCharset = this.adjutant.charsetClient();

            // 2. obtain maxPayload
            final long fileSize = channel.size();
            // (PacketUtils.MAX_PAYLOAD - 1 ) mean always send file bytes with single packet.
            final int maxAllowedPayload = Math.min(this.adjutant.obtainHostInfo().maxAllowedPayload()
                    , Packets.MAX_PAYLOAD - 1);
            final int maxPayload;
            if (fileSize < maxAllowedPayload) {
                maxPayload = (int) fileSize;
            } else {
                maxPayload = maxAllowedPayload;
            }
            // 3. create first packet buffer
            packet = this.adjutant.createPacketBuffer(maxPayload);

            long restFileBytes = fileSize;

            final ByteBuffer inputBuffer = ByteBuffer.allocate(2048);
            CharBuffer charBuffer;
            //4. read file bytes and write to packet
            while (channel.read(inputBuffer) > 0) {// 4.1 - read file bytes
                inputBuffer.flip();
                sentBytes += inputBuffer.remaining();

                //4.2 - decode file bytes with fileEncoding.
                charBuffer = fileEncoding.decode(inputBuffer);
                //4.3 - encode file bytes with client charset and  write encode file bytes
                packet.writeBytes(clientCharset.encode(charBuffer));
                //4.4 - dividing packet to multi single packet.
                while (packet.readableBytes() > maxPayload) {
                    ByteBuf tempPacket = packet.readRetainedSlice(maxPayload);
                    Packets.writePacketHeader(tempPacket, addAndGetSequenceId());
                    sink.next(tempPacket);

                    restFileBytes -= maxPayload;

                    if (restFileBytes > maxPayload) {
                        tempPacket = this.adjutant.createPacketBuffer(maxPayload);
                    } else {
                        tempPacket = this.adjutant.createPacketBuffer((int) restFileBytes);
                    }
                    tempPacket.writeBytes(packet);

                    packet.release();
                    packet = tempPacket;
                }

                //4.5 -  clear inputBuffer for next read.
                inputBuffer.clear();

            }
            Packets.writePacketHeader(packet, addAndGetSequenceId());
            sink.next(packet);
            if (packet.readableBytes() > Packets.HEADER_SIZE) {
                //5. write empty packet for end
                sink.next(Packets.createEmptyPacket(this.adjutant.allocator(), addAndGetSequenceId()));
            }

        } catch (Throwable e) {
            if (packet != null) {
                packet.release();
            }
            String message = String.format("Local file[%s] read error,have sent %s bytes.", localPath, sentBytes);
            addError(new LocalFileException(localPath, sentBytes, message, e));
            // send empty packet for end
            sink.next(Packets.createEmptyPacket(this.adjutant.allocator(), addAndGetSequenceId()));
        } finally {
            sink.complete();
        }


    }


    /**
     * @see SingleModeBatchUpdateSink#internalNextUpdate(ResultStates)
     */
    private void sendStaticCommand(final StaticStmt stmt) throws SQLException {
        // reset sequence_id
        this.updateSequenceId(-1);
        this.packetPublisher = Flux.fromIterable(
                QueryCommandWriter.createStaticCommand(stmt, this::addAndGetSequenceId, this.adjutant)
        );
    }

    /**
     * @see BindableSingleModeBatchUpdateSink#internalNextUpdate(ResultStates)
     */
    private void sendBindableCommand(final String sql, final List<BindValue> paramGroup) throws SQLException {
        // reset sequence_id
        this.updateSequenceId(-1);
        BindStmt bindStmt = Stmts.multi(sql, paramGroup);
        this.packetPublisher = Flux.fromIterable(
                QueryCommandWriter.createBindableCommand(bindStmt, this::addAndGetSequenceId, this.adjutant)
        );
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


    /*################################## blow private static class ##################################*/

    private interface DownstreamSink {

        void error(JdbdException e);

        /**
         * <p>
         * This method maybe invoke below methods:
         *     <ul>
         *         <li>{@link #sendStaticCommand(StaticStmt)}</li>
         *         <li>{@link #sendBindableCommand(String, List)}</li>
         *     </ul>
         * </p>
         *
         * @return true:task end.
         */
        boolean nextUpdate(ResultStates states);

        /**
         * <p>
         * This method maybe invoke below methods:
         *     <ul>
         *         <li>{@link #sendStaticCommand(StaticStmt)}</li>
         *         <li>{@link #sendBindableCommand(String, List)}</li>
         *     </ul>
         * </p>
         *
         * @return true: ResultSet end
         */
        boolean readResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);

        void complete();

        boolean hasMoreResult();

    }

    private interface SingleModeBatchDownstreamSink extends DownstreamSink {

        boolean hasMoreGroup();

        /**
         * @return true: task end
         */
        boolean sendCommand();
    }


    private static abstract class AbstractDownstreamSink implements DownstreamSink, ResultRowSink_0 {

        final ComQueryTask task;

        private ResultSetReader skipResultSetReader;

        private ResultStates lastResultStates;

        AbstractDownstreamSink(ComQueryTask task) {
            this.task = task;
        }

        @Override
        public final boolean nextUpdate(final ResultStates states) {
            this.lastResultStates = states;
            return this.internalNextUpdate(states);
        }

        @Override
        public final boolean readResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean resultSetEnd;
            if (this.task.hasError() && this.lastResultSetEnd()) {
                resultSetEnd = this.skipResultSet(cumulateBuffer, serverStatusConsumer);
            } else {
                resultSetEnd = this.internalReadResultSet(cumulateBuffer, serverStatusConsumer);
            }
            return resultSetEnd;
        }

        @Override
        public final void next(ResultRow row) {
            if (this.skipResultSetReader == null) {
                this.internalNext(row);
            }
        }

        @Override
        public final boolean isCancelled() {
            return this.task.hasError() || this.internalIsCancelled();
        }

        @Override
        public final void accept(ResultStates status) {
            this.lastResultStates = status;
            if (!this.lastResultSetEnd()) {
                this.internalAccept(status);
            }
        }

        @Override
        public final boolean hasMoreResult() {
            return Objects.requireNonNull(this.lastResultStates, "this.lastResultStatus")
                    .hasMoreResult();
        }

        /**
         * @return true : task end.
         */
        abstract boolean internalNextUpdate(ResultStates status);

        /**
         * @return true: ResultSet end
         */
        abstract boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);

        /**
         * @return true:  ResultSet end
         */
        final boolean skipResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            ResultSetReader resultSetReader = this.skipResultSetReader;
            if (resultSetReader == null) {
                resultSetReader = this.createResultSetReader();
                this.skipResultSetReader = resultSetReader;
            }
            return resultSetReader.read(cumulateBuffer, serverStatusConsumer);
        }

        final ResultSetReader createResultSetReader() {
            return ResultSetReaderBuilder.builder()
                    .rowSink(this)
                    .adjutant(this.task.adjutant)
                    .sequenceIdUpdater(this.task::updateSequenceId)

                    .errorConsumer(this.task::addError)
                    .resettable(true)// Text Protocol can support Reader resettable
                    .build(TextResultSetReader.class);
        }


        abstract boolean lastResultSetEnd();

        /**
         * <p>
         * if {@link #lastResultSetEnd()} is {@code true} don't invoke again.
         * </p>
         */
        abstract void internalAccept(ResultStates status);

        void internalNext(ResultRow row) {
            assertNotSupportSubClass();
        }

        boolean internalIsCancelled() {
            assertNotSupportSubClass();
            return true;
        }


        private void assertNotSupportSubClass() {
            if (this instanceof QueryDownstreamSink
                    || this instanceof AbstractMultiResultDownstreamSink) {
                throw new IllegalStateException(String.format("%s not override method.", this));
            }
        }

        @Override
        public final String toString() {
            return this.getClass().getSimpleName();
        }


    }

    /**
     * <p>
     * This static inner class is base class of below classes:
     *     <ul>
     *         <li>{@link SingleModeBatchUpdateSink}</li>
     *         <li>{@link BindableSingleModeBatchUpdateSink}</li>
     *     </ul>
     * </p>
     */
    private static abstract class AbstractSingleModeBatchUpdateSink extends AbstractDownstreamSink {

        final FluxSink<ResultStates> sink;

        private ResultStates queryStatus;

        AbstractSingleModeBatchUpdateSink(ComQueryTask task, FluxSink<ResultStates> sink) {
            super(task);
            this.sink = sink;
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.queryStatus != null;
        }

        @Override
        final boolean internalNextUpdate(ResultStates status) {
            final boolean taskEnd;
            if (this.task.hasError()) {
                taskEnd = !status.hasMoreResult();
            } else if (status.hasMoreResult()) {
                taskEnd = false;
                if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsBatchUpdateMultiError);
                } else {
                    this.task.addError(TaskUtils.createBatchUpdateMultiError());
                }
            } else {
                this.sink.next(status); // drain to downstream
                if (this instanceof SingleModeBatchDownstreamSink) {
                    final SingleModeBatchDownstreamSink sink = (SingleModeBatchDownstreamSink) this;
                    taskEnd = !sink.hasMoreGroup() || sink.sendCommand();
                } else {
                    taskEnd = true;
                }

            }
            return taskEnd;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean resultSetEnd;
            resultSetEnd = this.skipResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                final ResultStates status = Objects.requireNonNull(this.queryStatus, "this.queryStatus");
                if (status.hasMoreResult()) {
                    if (this.task.containException(SubscribeException.class)) {
                        this.task.replaceIfNeed(TaskUtils::replaceAsBatchUpdateMultiError);
                    } else {
                        this.task.addError(TaskUtils.createBatchUpdateMultiError());
                    }
                } else {
                    if (!this.task.containException(SubscribeException.class)) {
                        this.task.addError(TaskUtils.createBatchUpdateQueryError());
                    }

                }
            }
            return resultSetEnd;
        }

        @Override
        final void internalNext(ResultRow row) {
            //no-op
        }

        @Override
        final boolean internalIsCancelled() {
            return true;
        }

        @Override
        final void internalAccept(final ResultStates status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s resultSetStatus non-null.", this));
            }
        }

        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.complete();
        }

    }// AbstractBatchUpdateDownstreamSink


    /**
     * <p>
     * This static inner class is underlying api implementation downstream sink of below methods:
     *     <ul>
     *         <li>{@link StaticStatement#executeQuery(String)}</li>
     *         <li>{@link StaticStatement#executeQuery(String, Consumer)}</li>
     *         <li>{@link BindStatement#executeQuery()}</li>
     *         <li>{@link BindStatement#executeQuery(Consumer)}</li>
     *     </ul>
     * </p>
     *
     * @see #ComQueryTask(StaticStmt, FluxSink, TaskAdjutant)
     * @see #ComQueryTask(FluxSink, BindStmt, TaskAdjutant)
     */
    private final static class QueryDownstreamSink extends AbstractDownstreamSink {

        private final FluxSink<ResultRow> sink;

        private final Consumer<ResultStates> statusConsumer;

        private final ResultSetReader resultSetReader;

        private ResultStates queryStatus;

        /**
         * @see #ComQueryTask(StaticStmt, FluxSink, TaskAdjutant)
         * @see #ComQueryTask(FluxSink, BindStmt, TaskAdjutant)
         */
        private QueryDownstreamSink(final ComQueryTask task, FluxSink<ResultRow> sink, StaticStmt stmt) {
            super(task);
            task.assertSingleMode(this);
            this.sink = sink;
            this.statusConsumer = stmt.getStatusConsumer();
            this.resultSetReader = this.createResultSetReader();
        }

        @Override
        final boolean internalNextUpdate(final ResultStates status) {
            final boolean hasMoreResult = status.hasMoreResult();
            if (hasMoreResult) {
                if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsQueryMultiError);
                } else {
                    this.task.addError(TaskUtils.createQueryMultiError());
                }
            } else if (!this.task.containException(SubscribeException.class)) {
                this.task.addError(TaskUtils.createQueryUpdateError());
            }
            return !hasMoreResult;  // here ,maybe ,sql is that call stored procedure,skip rest results.
        }


        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean resultSetEnd;
            resultSetEnd = this.resultSetReader.read(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                final ResultStates states = Objects.requireNonNull(this.queryStatus, "this.queryStatus");
                if (states.hasMoreResult()) {
                    // here ,sql is that call stored procedure,skip rest results.
                    if (this.task.containException(SubscribeException.class)) {
                        this.task.replaceIfNeed(TaskUtils::replaceAsQueryMultiError);
                    } else {
                        this.task.addError(TaskUtils.createQueryMultiError());
                    }

                }
            }
            return resultSetEnd;
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.queryStatus != null;
        }

        @Override
        final void internalAccept(final ResultStates status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s resultSetStatus non-null.", this));
            }
        }

        /**
         * @see ResultRowSink_0#next(ResultRow)
         */
        @Override
        final void internalNext(ResultRow row) {
            this.sink.next(row);
        }

        /**
         * @see ResultRowSink_0#isCancelled()
         */
        @Override
        final boolean internalIsCancelled() {
            return this.sink.isCancelled();
        }


        @Override
        public final void error(final JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            try {
                // invoke user ResultStates Consumer.
                this.statusConsumer.accept(Objects.requireNonNull(this.queryStatus, "this.status"));
                this.sink.complete();
            } catch (Throwable e) {
                this.sink.error(ResultStatusConsumerException.create(this.statusConsumer, e));
            }
        }


    }// QueryDownstreamSink

    /**
     * <p>
     * This static inner class is underlying api implementation downstream sink of below methods:
     *     <ul>
     *         <li>{@link StaticStatement#executeUpdate(String)}</li>
     *         <li>{@link BindStatement#executeUpdate()}</li>
     *     </ul>
     * </p>
     *
     * @see #ComQueryTask(StaticStmt, MonoSink, TaskAdjutant)
     * @see #ComQueryTask(MonoSink, BindStmt, TaskAdjutant)
     */
    private static final class UpdateDownstreamSink extends AbstractDownstreamSink {

        private final MonoSink<ResultStates> sink;

        // update result status
        private ResultStates updateStaus;

        // query result status
        private ResultStates queryStatus;

        /**
         * @see #ComQueryTask(StaticStmt, MonoSink, TaskAdjutant)
         * @see #ComQueryTask(MonoSink, BindStmt, TaskAdjutant)
         */
        private UpdateDownstreamSink(final ComQueryTask task, MonoSink<ResultStates> sink) {
            super(task);
            task.assertSingleMode(this);
            this.sink = sink;
        }

        @Override
        final boolean internalNextUpdate(final ResultStates status) {
            if (this.updateStaus == null && !this.task.hasError()) {
                this.updateStaus = status;
            }
            final boolean taskEnd;
            if (status.hasMoreResult()) {
                if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsUpdateMultiError);
                } else {
                    this.task.addError(TaskUtils.createUpdateMultiError());
                }
                taskEnd = false;
            } else {
                taskEnd = true;
            }
            return taskEnd;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean resultSetEnd;
            resultSetEnd = this.skipResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                final ResultStates status = Objects.requireNonNull(this.queryStatus, "this.queryStatus");
                final boolean hasMorResult = status.hasMoreResult();
                if (hasMorResult) {
                    if (this.task.containException(SubscribeException.class)) {
                        this.task.replaceIfNeed(TaskUtils::replaceAsUpdateMultiError);
                    } else {
                        this.task.addError(TaskUtils.createUpdateMultiError());
                    }
                } else if (!this.task.containException(SubscribeException.class)) {
                    this.task.addError(TaskUtils.createUpdateQueryError());
                }
            }
            return resultSetEnd;
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.queryStatus != null;
        }

        @Override
        final void internalAccept(ResultStates status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s queryStatus non-null.", this));
            }
        }

        @Override
        public final void error(final JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.success(Objects.requireNonNull(this.updateStaus, "this.updateStaus"));
        }


    }// UpdateDownstreamSink


    /**
     * <p>
     * This static inner class is one of underlying implementation downstream sink of below methods.
     * <ul>
     *     <li>{@link StaticStatement#executeBatch(List)}</li>
     * </ul>
     * only when stmtList size less than 4 ,use this sink , or use {@link MultiModeBatchUpdateSink}
     * </p>
     *
     * @see MultiModeBatchUpdateSink
     */
    private static final class SingleModeBatchUpdateSink extends AbstractSingleModeBatchUpdateSink
            implements SingleModeBatchDownstreamSink {

        private final List<StaticStmt> stmtList;

        //start from 1 .
        private int index = 1;

        /**
         * @see #ComQueryTask(List, FluxSink, TaskAdjutant)
         * @see #ComQueryTask(FluxSink, BindStmt, TaskAdjutant)
         */
        private SingleModeBatchUpdateSink(final ComQueryTask task, List<StaticStmt> stmtList
                , FluxSink<ResultStates> sink) {
            super(task, sink);
            if (task.mode != Mode.SINGLE_STMT) {
                throw new IllegalArgumentException(String.format("%s isn't %s.", task, Mode.SINGLE_STMT));
            }
            if (stmtList.size() > 3) {
                throw new IllegalArgumentException("stmtList size error,please use multi-statement");
            }
            this.stmtList = stmtList;
        }

        @Override
        public final boolean hasMoreGroup() {
            return this.index < this.stmtList.size();
        }

        @Override
        public boolean sendCommand() {
            final boolean taskEnd;
            final int groupIndex = this.index++;
            if (groupIndex < this.stmtList.size()) {
                try {
                    this.task.sendStaticCommand(this.stmtList.get(groupIndex));
                } catch (Throwable e) {
                    this.task.addError(MySQLExceptions.wrap(e));
                }
                taskEnd = this.task.hasError();
            } else {
                taskEnd = true;
            }
            return taskEnd;
        }

    }// SingleModeBatchUpdateSink

    /**
     * <p>
     * This static inner class is one of underlying implementation downstream sink of below methods.
     * <ul>
     *     <li>{@link BindStatement#executeBatch()}</li>
     * </ul>
     * only when stmtList size less than 4 ,use this sink , or use {@link MultiModeBatchUpdateSink}
     * </p>
     *
     * @see MultiModeBatchUpdateSink
     */
    private static final class BindableSingleModeBatchUpdateSink extends AbstractSingleModeBatchUpdateSink
            implements SingleModeBatchDownstreamSink {

        private final String sql;

        private final List<List<BindValue>> groupList;

        // start from 1
        private int index = 1;

        /**
         * @see #ComQueryTask(FluxSink, BindBatchStmt, TaskAdjutant)
         */
        private BindableSingleModeBatchUpdateSink(final ComQueryTask task, final BindBatchStmt wrapper
                , FluxSink<ResultStates> sink) {
            super(task, sink);
            this.sql = wrapper.getSql();
            this.groupList = wrapper.getGroupList();
            if (this.groupList.size() > 3) {
                throw new IllegalArgumentException("groupList size error.");
            }

        }

        @Override
        public final boolean hasMoreGroup() {
            return this.index < this.groupList.size();
        }

        @Override
        public final boolean sendCommand() {
            final boolean taskEnd;
            final int groupIndex = this.index++;
            if (groupIndex < this.groupList.size()) {
                try {
                    this.task.sendBindableCommand(this.sql, this.groupList.get(groupIndex));
                } catch (Throwable e) {
                    this.task.addError(MySQLExceptions.wrap(e));
                }
                taskEnd = this.task.hasError();
            } else {
                taskEnd = true;
            }
            return taskEnd;
        }

    }//BindableSingleModeBatchUpdateSink


    /**
     * <p>
     * This static inner class is one of underlying implementation downstream sink of below methods.
     * <ul>
     *     <li>{@link StaticStatement#executeBatch(List)}</li>
     *     <li>{@link BindStatement#executeBatch()}</li>
     * </ul>
     * only when stmtList size great than 3 ,use this sink , or use below:
     * <ul>
     *     <li>{@link SingleModeBatchUpdateSink}</li>
     *     <li>{@link BindableSingleModeBatchUpdateSink}</li>
     * </ul>
     * </p>
     *
     * @see SingleModeBatchUpdateSink
     * @see BindableSingleModeBatchUpdateSink
     */
    private static final class MultiModeBatchUpdateSink extends AbstractDownstreamSink {

        private final FluxSink<ResultStates> sink;

        // query result status
        private ResultStates queryStatus;

        // start from 0
        private int resultSequenceId = 0;

        /**
         * @see #ComQueryTask(List, FluxSink, TaskAdjutant)
         * @see #ComQueryTask(FluxSink, BindBatchStmt, TaskAdjutant)
         */
        private MultiModeBatchUpdateSink(final ComQueryTask task, FluxSink<ResultStates> sink) {
            super(task);
            if (task.mode == Mode.SINGLE_STMT) {
                throw new IllegalStateException(String.format("mode[%s] error.", task.mode));
            }
            this.sink = sink;
        }

        @Override
        final boolean internalNextUpdate(final ResultStates status) {
            final int currentSequenceId = this.resultSequenceId++;
            if (!this.task.hasError()) {
                if (currentSequenceId < this.task.sqlCount) {
                    this.sink.next(status);// drain to downstream
                } else {
                    this.task.addError(TaskUtils.createBatchUpdateMultiError());
                }
            }
            return !status.hasMoreResult();
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean resultSetEnd;
            resultSetEnd = this.skipResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                this.queryStatus = null; // clear for next query
                final int currentSequenceId = this.resultSequenceId++;
                if (currentSequenceId < this.task.sqlCount) {
                    if (!this.task.containException(SubscribeException.class)) {
                        this.task.addError(TaskUtils.createBatchUpdateQueryError());
                    }
                } else if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsBatchUpdateMultiError);
                } else {
                    this.task.addError(TaskUtils.createBatchUpdateMultiError());
                }
            }
            return resultSetEnd;
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.queryStatus != null;
        }

        @Override
        final void internalAccept(final ResultStates status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s resultStatus non-null.", this));
            }
        }


        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.complete();
        }

    }// MultiModeBatchUpdateSink


    /**
     * <p>
     * This class is base class of below classes:
     *     <ul>
     *         <li>{@link MultiResultDownstreamSink}</li>
     *         <li>{@link SingleModeBatchBindMultiResultSink}</li>
     *         <li>{@link SingleModeBatchMultiResultSink}</li>
     *     </ul>
     * </p>
     */
    private static abstract class AbstractMultiResultDownstreamSink extends AbstractDownstreamSink {

        final MultiResultSink sink;

        final ResultSetReader resultSetReader;

        // query result status
        ResultStates queryStatus;

        QuerySink querySink;

        AbstractMultiResultDownstreamSink(ComQueryTask task, MultiResultSink sink) {
            super(task);
            this.sink = sink;
            this.resultSetReader = this.createResultSetReader();
        }

        @Override
        final boolean internalNextUpdate(final ResultStates status) {
            final boolean taskEnd;
            if (this.task.hasError()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{} downstream[{}] has error", this.task, this);
                }
                taskEnd = !status.hasMoreResult();
            } else {
                this.sink.nextUpdate(status);// drain to downstream

                if (status.hasMoreResult()) {
                    taskEnd = false;
                } else if (this instanceof SingleModeBatchDownstreamSink) {
                    final SingleModeBatchDownstreamSink sink = (SingleModeBatchDownstreamSink) this;
                    taskEnd = !sink.hasMoreGroup() || sink.sendCommand();
                } else {
                    taskEnd = true;
                }
            }
            return taskEnd;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final QuerySink querySink;
            if (this.querySink == null) {
                querySink = this.sink.nextQuery();
                this.querySink = querySink;
            } else {
                querySink = this.querySink;
            }

            final boolean resultSetEnd;
            resultSetEnd = this.resultSetReader.read(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                final ResultStates status = Objects.requireNonNull(this.queryStatus, "this.status");

                this.querySink = null; // clear for next query
                this.queryStatus = null;// clear for next query

                if (!this.task.hasError()) {
                    querySink.accept(status); // drain to downstream
                    querySink.complete();// drain to downstream

                }
            }
            return resultSetEnd;
        }


        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.complete();
        }


        @Override
        final boolean lastResultSetEnd() {
            return this.querySink == null;
        }

        @Override
        final void internalNext(ResultRow row) {
            final QuerySink querySink = this.querySink;
            if (querySink == null) {
                throw new NullPointerException("this.querySink");
            }
            querySink.next(row);
        }

        @Override
        final boolean internalIsCancelled() {
            final QuerySink querySink = this.querySink;
            if (querySink == null) {
                throw new NullPointerException("this.querySink");
            }
            return this.sink.isCancelled() || querySink.isCancelled();
        }

        @Override
        final void internalAccept(ResultStates states) {
            if (this.queryStatus != null) {
                // here,bug
                throw new IllegalStateException(String.format("%s.status not null.", this));
            }
            this.queryStatus = states;
        }


    }

    /**
     * <p>
     * This class is one of underlying api implementation downstream sink of below methods:
     *     <ul>
     *         <li>{@link StaticStatement#executeAsMulti(List)}</li>
     *         <li>{@link StaticStatement#executeAsFlux(List)}</li>
     *         <li>{@link BindStatement#executeBatchAsMulti()}</li>
     *         <li>{@link BindStatement#executeBatchAsFlux()}</li>
     *         <li>{@link MultiStatement#executeBatchAsMulti()}</li>
     *         <li>{@link MultiStatement#executeBatchAsFlux()}</li>
     *     </ul>
     * </p>
     * <p>
     *     When {@link #mode} isn't {@link Mode#SINGLE_STMT}  use this class ,or use below:
     *     <ul>
     *         <li>{@link SingleModeBatchBindMultiResultSink}</li>
     *         <li>{@link SingleModeBatchMultiResultSink}</li>
     *     </ul>
     * </p>
     */
    private static final class MultiResultDownstreamSink extends AbstractMultiResultDownstreamSink {

        /**
         * @see #ComQueryTask(List, MultiResultSink, TaskAdjutant)
         * @see #ComQueryTask(MultiResultSink, BindBatchStmt, TaskAdjutant)
         */
        private MultiResultDownstreamSink(final ComQueryTask task, MultiResultSink sink) {
            super(task, sink);
        }


    }// MultiResultDownstreamSink

    /**
     * <p>
     * This class is one of underlying api implementation downstream sink of below methods:
     *     <ul>
     *         <li>{@link StaticStatement#executeAsMulti(List)}</li>
     *         <li>{@link StaticStatement#executeAsFlux(List)}</li>
     *     </ul>
     * </p>
     * <p>
     *     When {@link #mode} is {@link Mode#SINGLE_STMT}  use this class ,or use below:
     *     <ul>
     *         <li>{@link MultiResultDownstreamSink}</li>
     *     </ul>
     * </p>
     */
    private static final class SingleModeBatchMultiResultSink extends AbstractMultiResultDownstreamSink
            implements SingleModeBatchDownstreamSink {

        private final List<StaticStmt> stmtList;

        //start from 1 .
        private int index = 1;

        private SingleModeBatchMultiResultSink(final ComQueryTask task, List<StaticStmt> stmtList
                , MultiResultSink sink) {
            super(task, sink);
            if (task.mode != Mode.SINGLE_STMT) {
                throw new IllegalArgumentException("mode error");
            }
            if (stmtList.size() > 3) {
                throw new IllegalArgumentException("stmtList size error.");
            }
            this.stmtList = stmtList;
        }

        @Override
        public final boolean hasMoreGroup() {
            return this.index < this.stmtList.size();
        }

        @Override
        public final boolean sendCommand() {
            final boolean taskEnd;
            final int currentIndex = this.index++;
            if (currentIndex < this.stmtList.size()) {
                try {
                    this.task.sendStaticCommand(this.stmtList.get(currentIndex));
                } catch (Throwable e) {
                    this.task.addError(JdbdExceptions.wrap(e));
                }
                taskEnd = this.task.hasError();
            } else {
                taskEnd = true;
            }
            return taskEnd;
        }


    }// SingleModeBatchMultiResultSink


    /**
     * <p>
     * This class is one of underlying api implementation downstream sink of below methods:
     *     <ul>
     *         <li>{@link BindStatement#executeBatchAsMulti()}</li>
     *         <li>{@link BindStatement#executeBatchAsFlux()}</li>
     *     </ul>
     * </p>
     * <p>
     *     When {@link #mode} is {@link Mode#SINGLE_STMT}  use this class ,or use below:
     *     <ul>
     *         <li>{@link MultiResultDownstreamSink}</li>
     *     </ul>
     * </p>
     */
    private static final class SingleModeBatchBindMultiResultSink extends AbstractMultiResultDownstreamSink
            implements SingleModeBatchDownstreamSink {

        private final String sql;

        private final List<List<BindValue>> groupList;

        // start from 1 .
        private int index = 1;

        /**
         * @see #ComQueryTask(MultiResultSink, BindBatchStmt, TaskAdjutant)
         */
        private SingleModeBatchBindMultiResultSink(final ComQueryTask task, MultiResultSink sink
                , BindBatchStmt wrapper) {
            super(task, sink);
            this.sql = wrapper.getSql();
            this.groupList = wrapper.getGroupList();
        }

        @Override
        public final boolean hasMoreGroup() {
            return this.index < this.groupList.size();
        }

        @Override
        public final boolean sendCommand() {
            final boolean taskEnd;
            final int groupIndex = this.index++;
            if (groupIndex < this.groupList.size()) {
                try {
                    this.task.sendBindableCommand(this.sql, this.groupList.get(groupIndex));
                } catch (Throwable e) {
                    this.task.addError(MySQLExceptions.wrap(e));
                }
                taskEnd = this.task.hasError();
            } else {
                taskEnd = true;
            }
            return taskEnd;
        }


    }// SingleModeBatchBindMultiResultSink


    /**
     * invoke this method after invoke {@link Packets#hasOnePacket(ByteBuf)}.
     *
     * @see #decode(ByteBuf, Consumer)
     */
    static ComQueryResponse detectComQueryResponseType(final ByteBuf cumulateBuffer, final int negotiatedCapability) {
        int readerIndex = cumulateBuffer.readerIndex();
        final int payloadLength = Packets.getInt3(cumulateBuffer, readerIndex);
        // skip header
        readerIndex += Packets.HEADER_SIZE;
        ComQueryResponse responseType;
        final boolean metadata = (negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;

        switch (Packets.getInt1AsInt(cumulateBuffer, readerIndex++)) {
            case 0: {
                if (metadata && Packets.obtainLenEncIntByteCount(cumulateBuffer, readerIndex) + 1 == payloadLength) {
                    responseType = ComQueryResponse.TEXT_RESULT;
                } else {
                    responseType = ComQueryResponse.OK;
                }
            }
            break;
            case ErrorPacket.ERROR_HEADER:
                responseType = ComQueryResponse.ERROR;
                break;
            case Packets.LOCAL_INFILE:
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
