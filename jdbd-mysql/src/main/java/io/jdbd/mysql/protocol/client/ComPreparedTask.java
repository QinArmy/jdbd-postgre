package io.jdbd.mysql.protocol.client;

import io.jdbd.*;
import io.jdbd.mysql.BindValue;
import io.jdbd.mysql.MySQLBindValue;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.mysql.util.MySQLCollectionUtils;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.vendor.LongParameterException;
import io.jdbd.vendor.TaskSignal;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.util.*;
import java.util.function.Consumer;

/**
 * <p>  code navigation :
 *     <ol>
 *         <li>decode entrance method : {@link #internalDecode(ByteBuf, Consumer)}</li>
 *         <li>send COM_STMT_PREPARE : {@link #internalStart(TaskSignal)} </li>
 *         <li>read COM_STMT_PREPARE Response : {@link #readPrepareResponse(ByteBuf)}
 *              <ol>
 *                  <li>read parameter meta : {@link #readPrepareParameterMeta(ByteBuf, Consumer)}</li>
 *                  <li>read prepare column meta : {@link #readPrepareColumnMeta(ByteBuf, Consumer)}</li>
 *              </ol>
 *         </li>
 *         <li>send COM_STMT_EXECUTE :
 *              <ul>
 *                  <li>{@link #executeStatement()}</li>
 *                  <li> {@link PrepareExecuteCommandWriter#writeCommand(List)}</li>
 *              </ul>
 *         </li>
 *         <li>read COM_STMT_EXECUTE Response : {@link #readExecuteResponse(ByteBuf, Consumer)}</li>
 *         <li>read Binary Protocol ResultSet Row : {@link BinaryResultSetReader#read(ByteBuf, Consumer)}</li>
 *         <li>send COM_STMT_CLOSE : {@link #closeStatement()}</li>
 *         <li>COM_STMT_CLOSE : {@link #createCloseStatementPacket()}</li>
 *     </ol>
 * </p>
 *
 * @see PrepareExecuteCommandWriter
 * @see BinaryResultSetReader
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase_ps.html">Prepared Statements</a>
 */
final class ComPreparedTask extends MySQLCommunicationTask {


    static Flux<ResultRow> query(StatementWrapper wrapper, MySQLTaskAdjutant adjutant) {

        return Flux.create(sink -> {

            // ComPreparedTask reference is hold by MySQLCommTaskExecutor.
            new ComPreparedTask(adjutant, new QuerySink(wrapper.getSql()
                    , wrapper.getParameterGroup(), sink, wrapper.getStatesConsumer()))
                    .submit(sink::error);
        });
    }

    static Mono<ResultStates> update(StatementWrapper wrapper, MySQLTaskAdjutant adjutant) {

        return Mono.create(sink -> {

            // ComPreparedTask reference is hold by MySQLCommTaskExecutor.
            new ComPreparedTask(adjutant, new UpdateSink(wrapper.getSql(), wrapper.getParameterGroup(), sink))
                    .submit(sink::error);
        });
    }

    static Flux<ResultStates> batchUpdate(StatementWrapper wrapper, MySQLTaskAdjutant adjutant) {
        return Flux.create(sink -> {
            new ComPreparedTask(adjutant, new BatchUpdateSink(wrapper.getSql(), wrapper.getParameterGroupList(), sink))
                    .submit(sink::error);
        });
    }

    private static final Logger LOG = LoggerFactory.getLogger(ComPreparedTask.class);


    private final DownStreamSink downStreamSink;

    private int statementId;

    private Phase phase = Phase.PREPARED;

    private MySQLColumnMeta[] parameterMetas;

    private int parameterMetaIndex = -1;

    private MySQLColumnMeta[] prepareColumnMetas;

    private int columnMetaIndex = -1;

    private Publisher<ByteBuf> packetPublisher;

    private int cursorFetchSize = -1;

    private List<Throwable> errorList;

    private TaskSignal<ByteBuf> signal;


    private ComPreparedTask(MySQLTaskAdjutant adjutant, DownStreamSink sink) {
        super(adjutant);
        this.downStreamSink = sink;

    }


    @Nullable
    @Override
    public Publisher<ByteBuf> moreSendPacket() {
        Publisher<ByteBuf> publisher = this.packetPublisher;
        if (publisher != null) {
            this.packetPublisher = null;
        }
        return publisher;
    }



    /*################################## blow protected  method ##################################*/

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html">Protocol::COM_STMT_PREPARE</a>
     */
    @Override
    protected Publisher<ByteBuf> internalStart(TaskSignal<ByteBuf> signal) {
        assertPhase(Phase.PREPARED);

        this.signal = signal;
        final String sql = this.downStreamSink.sql;
        int payloadLength = 1 + (sql.length() * this.adjutant.obtainMaxBytesPerCharClient());
        ByteBuf packetBuffer = this.adjutant.createPacketBuffer(payloadLength);

        packetBuffer.writeByte(PacketUtils.COM_STMT_PREPARE); // command
        packetBuffer.writeCharSequence(sql, this.adjutant.obtainCharsetClient());// query

        PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId());

        this.phase = Phase.READ_PREPARE_RESPONSE;
        return Mono.just(packetBuffer);
    }

    /**
     * @see #decode(ByteBuf, Consumer)
     */
    @Override
    protected boolean internalDecode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        if (!PacketUtils.hasOnePacket(cumulateBuffer)) {
            return false;
        }
        boolean taskEnd = false, continueDecode = true;
        while (continueDecode) {
            switch (this.phase) {
                case READ_PREPARE_RESPONSE: {
                    taskEnd = readPrepareResponse(cumulateBuffer);
                    continueDecode = !taskEnd && PacketUtils.hasOnePacket(cumulateBuffer);
                }
                break;
                case READ_PREPARE_PARAM_META: {
                    if (readPrepareParameterMeta(cumulateBuffer, serverStatusConsumer)) {
                        this.phase = Phase.READ_PREPARE_COLUMN_META;
                        continueDecode = PacketUtils.hasOnePacket(cumulateBuffer);
                    } else {
                        continueDecode = false;
                    }
                }
                break;
                case READ_PREPARE_COLUMN_META: {
                    if (readPrepareColumnMeta(cumulateBuffer, serverStatusConsumer)) {
                        taskEnd = executeStatement(); // execute command
                        if (!taskEnd) {
                            this.phase = Phase.READ_EXECUTE_RESPONSE;
                        }
                    }
                    continueDecode = false;
                }
                break;
                case READ_EXECUTE_RESPONSE: {
                    // maybe modify this.phase
                    taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
                    continueDecode = false;
                }
                break;
                case READ_RESULT_SET: {
                    taskEnd = this.downStreamSink.readResultSet(cumulateBuffer, serverStatusConsumer);
                    continueDecode = false;
                }
                break;
                default:
                    throw new IllegalStateException(String.format("this.phase[%s] error.", this.phase));
            }
        }
        if (taskEnd) {
            closeStatement();
        }
        return taskEnd;
    }


    /**
     * @see #error(Throwable)
     */
    @Nullable
    @Override
    protected Publisher<ByteBuf> internalError(Throwable e) {
        Publisher<ByteBuf> publisher = null;
        if (e instanceof SQLBindParameterException) {
            publisher = handleSQLBindParameterException((SQLBindParameterException) e);
        }
        return publisher;
    }

    /*################################## blow private method ##################################*/

    /**
     * @return true: prepare error,task end.
     * @see #internalDecode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response">COM_STMT_PREPARE Response</a>
     */
    private boolean readPrepareResponse(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_PREPARE_RESPONSE);

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
        final int headFlag = PacketUtils.getInt1(cumulateBuffer, cumulateBuffer.readerIndex()); //1. status/error header
        boolean taskEnd;
        switch (headFlag) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetResults());
                this.downStreamSink.error(MySQLExceptionUtils.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                final int payloadStartIndex = cumulateBuffer.readerIndex();
                cumulateBuffer.skipBytes(1);//skip status
                this.statementId = PacketUtils.readInt4(cumulateBuffer);//2. statement_id
                resetColumnMeta(PacketUtils.readInt2(cumulateBuffer));//3. num_columns
                resetParameterMetas(PacketUtils.readInt2(cumulateBuffer));//4. num_params
                cumulateBuffer.skipBytes(1); //5. skip filler
                int prepareWarningCount = PacketUtils.readInt2(cumulateBuffer);//6. warning_count
                if (prepareWarningCount > 0 && LOG.isWarnEnabled()) {
                    LOG.warn("sql[{}] prepare occur {} warning.", this.downStreamSink.sql, prepareWarningCount);
                }
                if ((this.negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
                    throw new IllegalStateException("Not support CLIENT_OPTIONAL_RESULTSET_METADATA"); //7. metadata_follows
                }
                cumulateBuffer.readerIndex(payloadStartIndex + payloadLength); // to next packet,avoid tail filler.
                this.phase = Phase.READ_PREPARE_PARAM_META;
                taskEnd = false;
            }
            break;
            default: {
                throw MySQLExceptionUtils.createFatalIoException(
                        "Server send COM_STMT_PREPARE Response error. header        [%s]", headFlag);
            }
        }

        return taskEnd;
    }


    /**
     * @return true:read parameter meta end.
     * @see #readPrepareResponse(ByteBuf)
     * @see #internalDecode(ByteBuf, Consumer)
     * @see #resetParameterMetas(int)
     */
    private boolean readPrepareParameterMeta(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_PREPARE_PARAM_META);
        int parameterMetaIndex = this.parameterMetaIndex;
        final MySQLColumnMeta[] metaArray = Objects.requireNonNull(this.parameterMetas, "this.parameterMetas");
        if (parameterMetaIndex < metaArray.length) {
            parameterMetaIndex = AbstractComQueryTask.tryReadColumnMetas(cumulateBuffer
                    , parameterMetaIndex, this.adjutant, metaArray, this::updateSequenceId);
            this.parameterMetaIndex = parameterMetaIndex;
        }
        return parameterMetaIndex == metaArray.length && tryReadEof(cumulateBuffer, serverStatusConsumer);
    }


    /**
     * @return true:read column meta end.
     * @see #readPrepareColumnMeta(ByteBuf, Consumer)
     * @see #internalDecode(ByteBuf, Consumer)
     * @see #resetColumnMeta(int)
     */
    private boolean readPrepareColumnMeta(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (this.phase != Phase.READ_PREPARE_COLUMN_META) {
            throw new IllegalStateException(
                    String.format("this.phase[%s] isn't %s.", this.phase, Phase.READ_PREPARE_COLUMN_META));
        }

        final MySQLColumnMeta[] metaArray = Objects.requireNonNull(this.prepareColumnMetas, "this.prepareColumnMetas");
        int columnMetaIndex = this.columnMetaIndex;
        if (columnMetaIndex < metaArray.length) {
            columnMetaIndex = AbstractComQueryTask.tryReadColumnMetas(cumulateBuffer, columnMetaIndex
                    , this.adjutant, metaArray, this::updateSequenceId);
            this.columnMetaIndex = columnMetaIndex;
        }
        return columnMetaIndex == metaArray.length && tryReadEof(cumulateBuffer, serverStatusConsumer);
    }

    /**
     * @return false : need more cumulate
     * @see #readPrepareParameterMeta(ByteBuf, Consumer)
     * @see #readPrepareColumnMeta(ByteBuf, Consumer)
     */
    private boolean tryReadEof(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        boolean end = true;
        if ((this.negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) == 0) {
            if (PacketUtils.hasOnePacket(cumulateBuffer)) {
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
                EofPacket eof;
                eof = EofPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(eof.getStatusFags());
            } else {
                end = false;
            }
        }
        return end;
    }


    /**
     * @see #internalDecode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html">Protocol::COM_STMT_CLOSE</a>
     */
    private void closeStatement() {
        List<Throwable> errorList = this.errorList;
        if (!MySQLCollectionUtils.isEmpty(errorList)) {

        }
        ByteBuf packet = this.adjutant.createPacketBuffer(5);
        packet.writeByte(PacketUtils.COM_STMT_CLOSE);
        PacketUtils.writeInt4(packet, this.statementId);
        PacketUtils.writePacketHeader(packet, addAndGetSequenceId());

        this.packetPublisher = Mono.just(packet);
    }


    /**
     * @see #internalError(Throwable)
     * @see #executeStatement()
     */
    private void executeCommandWriterErrorEvent(Throwable e) {
        if (this.adjutant.inEventLoop()) {
            this.errorList.add(e);
        } else {
            this.adjutant.execute(() -> this.errorList.add(e));
        }
    }

    /**
     * @see #executeQueryStatement(QuerySink, MySQLColumnMeta[], StatementCommandWriter)
     */
    private void readResultSetErrorEvent(Throwable e) {
        if (e instanceof JdbdSQLException) {
            this.downStreamSink.error(e);
        } else {
            List<Throwable> errorList = this.errorList;
            if (errorList == null) {
                errorList = new ArrayList<>();
                this.errorList = errorList;
            }
            errorList.add(e);
        }
    }


    /**
     * @return true: task end.
     * @see #internalDecode(ByteBuf, Consumer)
     * @see #executeQueryStatement(QuerySink, MySQLColumnMeta[], StatementCommandWriter)
     */
    private boolean readExecuteResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_EXECUTE_RESPONSE);

        final int header = PacketUtils.getInt1(cumulateBuffer, cumulateBuffer.readerIndex() + PacketUtils.HEADER_SIZE);
        final boolean taskEnd;
        switch (header) {
            case ErrorPacket.ERROR_HEADER: {
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1(cumulateBuffer));

                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetResults());
                this.downStreamSink.error(MySQLExceptionUtils.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1(cumulateBuffer));

                OkPacket ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok.getStatusFags());
                // emit update result
                Publisher<ByteBuf> publisher = this.downStreamSink.emitUpdateResult(MySQLResultStates.from(ok)
                        , Objects.requireNonNull(this.parameterMetas, "this.parameterMetas"));
                this.packetPublisher = publisher;
                taskEnd = publisher == null;
            }
            break;
            default: {
                this.phase = Phase.READ_RESULT_SET;
                taskEnd = this.downStreamSink.readResultSet(cumulateBuffer, serverStatusConsumer);
            }
        }
        if (taskEnd && MySQLCollectionUtils.isEmpty(this.errorList)) {
            this.phase = Phase.STATEMENT_END;
        }
        return taskEnd;
    }


    /**
     * @return true : bind parameter error,task end.
     * @see #internalDecode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
     */
    private boolean executeStatement() {
        assertPhase(Phase.EXECUTE);

        final DownStreamSink downStreamSink = this.downStreamSink;

        final MySQLColumnMeta[] columnMetaArray = Objects.requireNonNull(
                this.prepareColumnMetas, "this.prepareColumnMetas");

        final MySQLColumnMeta[] parameterMetaArray = Objects.requireNonNull(
                this.parameterMetas, "this.parameterMetas");

        final StatementCommandWriter commandWriter = new PrepareExecuteCommandWriter(
                this.statementId, parameterMetaArray
                , columnMetaArray.length > 0, this::addAndGetSequenceId
                , this.adjutant, this::executeCommandWriterErrorEvent);

        boolean taskEnd = false;
        try {
            if (downStreamSink instanceof QuerySink) {
                if (columnMetaArray.length == 0) {
                    downStreamSink.error(SubscriptionNotMatchException.expectQuery());
                    taskEnd = true;
                } else {
                    executeQueryStatement((QuerySink) downStreamSink, parameterMetaArray, commandWriter);
                }
            } else if (downStreamSink instanceof UpdateSink) {
                if (columnMetaArray.length > 0) {
                    downStreamSink.error(SubscriptionNotMatchException.expectUpdate());
                    taskEnd = true;
                } else {
                    executeUpdateStatement((UpdateSink) downStreamSink, parameterMetaArray, commandWriter);
                }
            } else if (downStreamSink instanceof BatchUpdateSink) {
                if (columnMetaArray.length > 0) {
                    downStreamSink.error(SubscriptionNotMatchException.expectBatchUpdate());
                    taskEnd = true;
                } else {
                    taskEnd = executeBatchUpdateStatement((BatchUpdateSink) downStreamSink
                            , parameterMetaArray, commandWriter);
                }
            } else {
                throw new IllegalStateException(String.format("Unknown DownstreamSink[%s]", downStreamSink));
            }

            this.phase = Phase.READ_EXECUTE_RESPONSE;

        } catch (SQLBindParameterException e) {
            downStreamSink.error(e);
            taskEnd = true;
        }
        return taskEnd;
    }

    /**
     * @see #executeStatement()
     */
    private void executeQueryStatement(final QuerySink querySink, final MySQLColumnMeta[] parameterMetaArray
            , final StatementCommandWriter commandWriter) throws SQLBindParameterException {


        List<BindValue> parameterGroup = Objects.requireNonNull(querySink.parameterGroup
                , "querySink.parameterGroup");

        if (parameterGroup.isEmpty()) {
            parameterGroup = Collections.emptyList();
        } else {
            parameterGroup = prepareParameterGroup(parameterMetaArray, parameterGroup);
        }
        querySink.parameterGroup = parameterGroup;
        this.packetPublisher = commandWriter.writeCommand(parameterGroup); // write command

        querySink.resultSetReader = new BinaryResultSetReader(querySink.sink, querySink.statesConsumer
                , this.adjutant, this::updateSequenceId
                , this::readResultSetErrorEvent);
    }

    /**
     * @see #executeStatement()
     */
    private void executeUpdateStatement(final UpdateSink updateSink, final MySQLColumnMeta[] parameterMetaArray
            , final StatementCommandWriter commandWriter) throws SQLBindParameterException {
        List<BindValue> parameterGroup = Objects.requireNonNull(updateSink.parameterGroup
                , "updateSink.parameterGroup");
        if (parameterGroup.isEmpty()) {
            parameterGroup = Collections.emptyList();
        } else {
            parameterGroup = prepareParameterGroup(parameterMetaArray, parameterGroup);
        }
        updateSink.parameterGroup = parameterGroup;
        this.packetPublisher = commandWriter.writeCommand(parameterGroup); // write command
    }

    /**
     * @see #executeStatement()
     */
    private boolean executeBatchUpdateStatement(final BatchUpdateSink batchSink, final MySQLColumnMeta[] parameterMetaArray
            , final StatementCommandWriter commandWriter) throws SQLBindParameterException {
        List<List<BindValue>> groupList = Objects.requireNonNull(batchSink.groupList
                , "batchSink.groupList");


        final List<BindValue> parameterGroup;
        if (groupList.isEmpty()) {
            parameterGroup = Collections.emptyList();
        } else {
            parameterGroup = prepareParameterGroup(parameterMetaArray, groupList.get(0));
            groupList.set(0, parameterGroup);
            batchSink.index++;
        }
        final boolean taskEnd;
        if (parameterGroup.isEmpty()) {
            batchSink.error(SubscriptionNotMatchException.expectBatchUpdate());
            taskEnd = true;
        } else {
            this.packetPublisher = commandWriter.writeCommand(parameterGroup);
            taskEnd = false;
        }
        return taskEnd;
    }


    /**
     * @see #readPrepareColumnMeta(ByteBuf, Consumer)
     * @see #readExecuteResponse(ByteBuf, Consumer)
     */
    private void resetColumnMeta(final int columnCount) {
        this.prepareColumnMetas = new MySQLColumnMeta[columnCount];
        this.columnMetaIndex = 0;
    }

    /**
     * @see #readPrepareResponse(ByteBuf)
     */
    private void resetParameterMetas(int parameterCount) {
        this.parameterMetas = new MySQLColumnMeta[parameterCount];
        this.parameterMetaIndex = 0;
    }


    /**
     * @see #internalError(Throwable)
     */
    @Nullable
    private Publisher<ByteBuf> handleSQLBindParameterException(SQLBindParameterException e) {
        Publisher<ByteBuf> publisher = null;
        if (e instanceof LongParameterException) {
            // io.jdbd.mysql.protocol.client.PrepareLongParameterWriter.publishLonDataReadException
            publisher = Mono.just(createCloseStatementPacket());//1. close statement
        } else if (e instanceof BindParameterException) {

        } else {

        }
        return publisher;
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html">Protocol::COM_STMT_CLOSE</a>
     */
    private ByteBuf createCloseStatementPacket() {
        ByteBuf packet = this.adjutant.alloc().buffer(9);

        PacketUtils.writeInt3(packet, 5);
        packet.writeByte(addAndGetSequenceId());

        packet.writeByte(PacketUtils.COM_STMT_CLOSE);
        PacketUtils.writeInt4(packet, this.statementId);
        return packet;
    }


    private void assertPhase(Phase expectedPhase) {
        if (this.phase != expectedPhase) {
            throw new IllegalStateException(String.format("this.phase isn't %s.", adjutant));
        }
    }

    /*################################## blow private static method ##################################*/


    private static List<BindValue> prepareParameterGroup(final MySQLColumnMeta[] parameterMetaArray
            , final List<BindValue> parameterGroup) {

        final int size = parameterGroup.size();
        if (size != parameterMetaArray.length) {
            throw new SQLBindParameterException(
                    "Bind parameter count[%s] and prepare sql parameter count[%s] not match."
                    , parameterGroup.size(), parameterMetaArray.length);
        }

        List<BindValue> list = new ArrayList<>(parameterGroup);

        list.sort(Comparator.comparingInt(BindValue::getParamIndex));

        BindValue bindValue;
        for (int i = 0, index; i < size; i++) {
            bindValue = parameterGroup.get(i);
            index = bindValue.getParamIndex();
            if (index < i) {
                throw new BindParameterException(index, "Bind parameter[%s] duplication.", index);
            } else if (index != i) {
                throw new BindParameterException(i, "Bind parameter[%s] not set.", i);
            } else {
                MySQLType type = parameterMetaArray[i].mysqlType;
                if (bindValue.getType() != type) {
                    list.set(i, MySQLBindValue.create(bindValue, type));
                }
            }
        }

        return MySQLCollectionUtils.unmodifiableList(list);
    }


    /*################################## blow private static inner class ##################################*/


    private static abstract class DownStreamSink {

        private final String sql;

        StatementCommandWriter commandWriter;

        public DownStreamSink(String sql) {
            this.sql = sql;
        }

        abstract void error(Throwable e);

        @Nullable
        abstract Publisher<ByteBuf> emitUpdateResult(ResultStates resultStates, MySQLColumnMeta[] parameterMetaArray);

        abstract boolean readResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);
    }

    private static final class QuerySink extends DownStreamSink {

        private ResultSetReader resultSetReader;

        private List<BindValue> parameterGroup;

        private final FluxSink<ResultRow> sink;

        private final Consumer<ResultStates> statesConsumer;


        private QuerySink(String sql, List<BindValue> parameterGroup, FluxSink<ResultRow> sink
                , Consumer<ResultStates> statesConsumer) {
            super(sql);
            this.resultSetReader = null;
            this.parameterGroup = parameterGroup;
            this.sink = sink;
            this.statesConsumer = statesConsumer;
        }

        @Override
        void error(Throwable e) {
            this.sink.error(e);
        }

        @Override
        Publisher<ByteBuf> emitUpdateResult(final ResultStates resultStates
                , final MySQLColumnMeta[] parameterMetaArray) {
            throw new IllegalStateException(String.format("%s not support read update result.", this));
        }

        @Override
        boolean readResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
            return Objects.requireNonNull(this.resultSetReader, "this.resultSetReader")
                    .read(cumulateBuffer, serverStatusConsumer);
        }
    }

    private static final class UpdateSink extends DownStreamSink {

        private List<BindValue> parameterGroup;

        private final MonoSink<ResultStates> sink;

        private UpdateSink(String sql, List<BindValue> parameterGroup, MonoSink<ResultStates> sink) {
            super(sql);
            this.parameterGroup = parameterGroup;
            this.sink = sink;
        }

        @Override
        void error(Throwable e) {
            this.sink.error(e);
        }

        @Override
        Publisher<ByteBuf> emitUpdateResult(final ResultStates resultStates
                , final MySQLColumnMeta[] parameterMetaArray) {
            this.sink.success(resultStates);
            return null;
        }

        @Override
        boolean readResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
            throw new IllegalStateException(String.format("%s not support read result set.", this));
        }
    }

    private static final class BatchUpdateSink extends DownStreamSink {

        private final List<List<BindValue>> groupList;

        private int index = 0;

        private final FluxSink<ResultStates> sink;


        private BatchUpdateSink(String sql, List<List<BindValue>> groupList, FluxSink<ResultStates> sink) {
            super(sql);
            this.groupList = MySQLCollectionUtils.unmodifiableList(groupList);
            this.sink = sink;
        }

        @Override
        void error(Throwable e) {
            this.sink.error(e);
        }

        @Nullable
        @Override
        Publisher<ByteBuf> emitUpdateResult(final ResultStates resultStates
                , final MySQLColumnMeta[] parameterMetaArray) {

            this.sink.next(resultStates);

            final int index = this.index;

            Publisher<ByteBuf> publisher;
            if (index == this.groupList.size()) {
                publisher = null;
            } else {
                try {
                    List<BindValue> parameterGroup = this.groupList.get(index);
                    parameterGroup = prepareParameterGroup(parameterMetaArray, parameterGroup);
                    this.groupList.set(index, parameterGroup);
                    this.index++;

                    publisher = Objects.requireNonNull(this.commandWriter, "this.commandWriter")
                            .writeCommand(parameterGroup);
                } catch (SQLBindParameterException e) {
                    this.sink.error(e);
                    publisher = null;
                }
            }
            return publisher;

        }

        @Override
        boolean readResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
            throw new IllegalStateException(String.format("%s not support read result set.", this));
        }
    }


    enum Phase {
        PREPARED,
        READ_PREPARE_RESPONSE,
        READ_PREPARE_PARAM_META,
        READ_PREPARE_COLUMN_META,

        EXECUTE,
        READ_EXECUTE_RESPONSE,
        READ_RESULT_SET,

        STATEMENT_END,
        RESET_STMT,
        FETCH_RESULT,
        READ_FETCH_RESULT,
        WAIT_FOR_PARAMETER,
        CLOSE_STMT
    }


}
