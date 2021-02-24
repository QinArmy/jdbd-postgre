package io.jdbd.mysql.protocol.client;

import io.jdbd.*;
import io.jdbd.mysql.BindValue;
import io.jdbd.mysql.PrepareWrapper;
import io.jdbd.mysql.StmtWrapper;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.mysql.util.MySQLCollectionUtils;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.MultiResultsSink;
import io.jdbd.vendor.SQLStatement;
import io.jdbd.vendor.TaskSignal;
import io.jdbd.vendor.result.QuerySink;
import io.jdbd.vendor.result.ResultRowSink;
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
import java.util.function.Supplier;


final class ComQueryTask extends MySQLCommunicationTask {

    static MultiResults singleCommand(MySQLTaskAdjutant taskAdjutant, String command) {
        throw new UnsupportedOperationException();
    }

    static MultiResults multiCommands(MySQLTaskAdjutant taskAdjutant, List<String> commandList) {
        throw new UnsupportedOperationException();
    }

    static Mono<ResultStates> update(String sql, MySQLTaskAdjutant adjutant) {
        return Mono.create(sink -> {
        });
    }

    static Flux<ResultRow> query(String sql, Consumer<ResultStates> statesConsumer, MySQLTaskAdjutant adjutant) {
        return Flux.create(sink -> {
        });
    }

    static Flux<ResultStates> batchUpdate(List<String> sqlList, MySQLTaskAdjutant adjutant) {
        return Flux.create(sink -> {
        });
    }

    static Mono<ResultStates> prepareUpdate(StmtWrapper wrapper, MySQLTaskAdjutant adjutant) {
        return Mono.create(sink -> {
        });
    }

    static Flux<ResultRow> prepareQuery(StmtWrapper wrapper, MySQLTaskAdjutant adjutant) {
        return Flux.create(sink -> {
        });
    }

    static Flux<ResultStates> prepareBatchUpdate(PrepareWrapper wrapper, MySQLTaskAdjutant adjutant) {
        return Flux.create(sink -> {
        });
    }

    static MultiResults multiStatement(List<StmtWrapper> stmtWrapperList, MySQLTaskAdjutant adjutant) {
        return null;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTask.class);

    private final DownstreamSink downstreamSink;

    private final Mode mode;

    private final Supplier<Publisher<ByteBuf>> commandWriter;

    private final int sqlCount = 0;

    private int currentResultSequenceId = 1;

    private Phase phase;

    private Pair<Integer, ResultStates> lastResultStates;

    private Publisher<ByteBuf> packetPublisher;

    private List<JdbdException> errorList;

    private ResultSetReader dirtyResultSetReader;

    private ResultRowSink dirtyRowSink;

    private ComQueryTask(MySQLTaskAdjutant adjutant) {
        super(adjutant);
        this.commandWriter = this::createSimpleComQueryPackets;
        this.downstreamSink = null;
        this.mode = Mode.MULTI_STMT;
    }


    /*################################## blow package template method ##################################*/

    @Override
    protected Publisher<ByteBuf> internalStart(TaskSignal<ByteBuf> signal) {
        this.phase = Phase.READ_RESPONSE_RESULT_SET;
        return null;
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
                case LOCAL_INFILE_REQUEST: {
                    throw new IllegalStateException(String.format("%s phase[%s] error.", this, this.phase));
                }
                default:
                    throw MySQLExceptionUtils.createUnknownEnumException(this.phase);
            }
        }
        return taskEnd;
    }

    @Override
    public Publisher<ByteBuf> moreSendPacket() {
        final Publisher<ByteBuf> packetPublisher = this.packetPublisher;
        if (packetPublisher != null) {
            this.packetPublisher = null;
        }
        return packetPublisher;
    }


    /*################################## blow private method ##################################*/

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
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1(cumulateBuffer)); //  sequence_id
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetResults());
                addErrorForSqlError(error);
                taskEnd = true;
            }
            break;
            case OK: {
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
                OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok.getStatusFags());
                final ResultStates resultStates = MySQLResultStates.from(ok);
                final int resultSequenceId = this.currentResultSequenceId++;
                updateLastResultStates(resultSequenceId, resultStates);
                // emit update result.
                if (this.downstreamSink.skipRestResults()) {
                    if (!hasError(StatementException.class)) {
                        addError(new StatementException("Expect single statement ,but multi statement."));
                    }
                } else {
                    this.downstreamSink.nextUpdate(resultSequenceId, resultStates);
                }
                if (this.mode == Mode.SINGLE_STMT) {
                    taskEnd = resultSequenceId == this.sqlCount;
                } else {
                    taskEnd = !resultStates.hasMoreResults();
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
                throw MySQLExceptionUtils.createUnknownEnumException(response);
        }
        return taskEnd;
    }

    /**
     * @return true: task end.
     */
    private boolean readTextResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_TEXT_RESULT_SET);

        int resultSequenceId = this.currentResultSequenceId;
        final boolean resultSetEnd;
        if (this.dirtyResultSetReader != null || this.downstreamSink.skipRestResults()) {
            if (!hasException(StatementException.class)) {
                addError(new StatementException("Expect single statement ,but multi statement."));
            }
            resultSetEnd = skipTextResultSet(cumulateBuffer, serverStatusConsumer);
        } else {
            resultSetEnd = this.downstreamSink.readTextResultSet(resultSequenceId, cumulateBuffer
                    , serverStatusConsumer);
        }
        final boolean taskEnd;
        if (resultSetEnd) {
            // must update currentResultSequenceId after result set end.
            this.currentResultSequenceId++;
            this.phase = Phase.READ_RESPONSE_RESULT_SET;
            if (this.mode == Mode.SINGLE_STMT) {
                taskEnd = resultSequenceId == this.sqlCount;
            } else {
                taskEnd = !hasMoreResults();
            }
        } else {
            taskEnd = false;
        }
        return taskEnd;
    }


    private void addError(JdbdException e) {
        List<JdbdException> errorList = this.errorList;
        if (errorList == null) {
            errorList = new ArrayList<>();
            this.errorList = errorList;
        }
        errorList.add(e);
    }

    private JdbdException createException() {
        List<JdbdException> errorList = this.errorList;
        if (MySQLCollectionUtils.isEmpty(errorList)) {
            throw new IllegalStateException(String.format("%s No error,reject creat exception.", this));
        }
        JdbdException e;
        if (errorList.size() == 1) {
            e = errorList.get(0);
        } else {
            e = new JdbdCompositeException(errorList, "occur multi error.");
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
            e = MySQLExceptionUtils.createErrorPacketException(error);
        }
        addError(e);
    }

    private void updateLastResultStates(final int resultSequenceId, final ResultStates resultStates) {
        Pair<Integer, ResultStates> pair = this.lastResultStates;
        if (pair != null && pair.getFirst() != resultSequenceId - 1) {
            throw new IllegalStateException(String.format(
                    "%s lastResultStates[sequenceId:%s] but expect update to sequenceId:%s ."
                    , this, pair.getFirst(), resultSequenceId));
        }
        this.lastResultStates = new Pair<>(resultSequenceId, resultStates);
    }

    private boolean hasError() {
        return !MySQLCollectionUtils.isEmpty(this.errorList);
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
        if (!hasException(StatementException.class)) {
            addError(new StatementException("Expect single statement ,but multi statement."));
        }
    }

    private boolean hasMoreResults() {
        Pair<Integer, ResultStates> pair = this.lastResultStates;
        return pair != null && pair.getSecond().hasMoreResults();
    }

    private Publisher<ByteBuf> createSimpleComQueryPackets() {
        return Mono.empty();
    }

    /**
     * @see #readResponseResultSet(ByteBuf, Consumer)
     */
    private void sendLocalFile(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.LOCAL_INFILE_REQUEST);

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
        if (PacketUtils.readInt1(cumulateBuffer) != PacketUtils.LOCAL_INFILE) {
            throw new IllegalStateException(String.format("%s invoke sendLocalFile method error.", this));
        }
        String localFilePath;
        localFilePath = PacketUtils.readStringFixed(cumulateBuffer, payloadLength - 1
                , this.adjutant.obtainCharsetResults());

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
        ByteBuf packet = this.adjutant.alloc().buffer(PacketUtils.HEADER_SIZE);
        PacketUtils.writeInt3(packet, 0);
        packet.writeByte(addAndGetSequenceId());
        return packet;
    }

    private void sendStaticCommand(final String sql) {
        // result sequence_id
        this.updateSequenceId(-1);
        try {
            final byte[] commandBytes = sql.getBytes(this.adjutant.obtainCharsetClient());
            this.packetPublisher = Flux.create(sink -> sendBigStaticCommand(sink, commandBytes));
        } catch (Throwable e) {
            this.packetPublisher = Flux.error(e);
        }
    }

    /**
     * @see #sendStaticCommand(String)
     */
    private void sendBigStaticCommand(final FluxSink<ByteBuf> sink, final byte[] commandBytes) {
        ByteBuf packet;
        if (commandBytes.length < PacketUtils.MAX_PAYLOAD) {
            packet = this.adjutant.createPacketBuffer(1 + commandBytes.length);
        } else {
            packet = this.adjutant.createPacketBuffer(PacketUtils.MAX_PAYLOAD);
        }

        packet.writeByte(PacketUtils.COM_QUERY_HEADER);

        for (int offset = 0, length; offset < commandBytes.length; ) {
            if (offset == 0) {
                length = Math.min(PacketUtils.MAX_PAYLOAD - 1, commandBytes.length);
            } else {
                length = Math.min(PacketUtils.MAX_PAYLOAD, commandBytes.length - offset);
            }
            packet.writeBytes(commandBytes, offset, length);
            offset += length;

            if (packet.readableBytes() == PacketUtils.MAX_PACKET) {
                PacketUtils.writePacketHeader(packet, addAndGetSequenceId());
                sink.next(packet);
                packet = this.adjutant.createPacketBuffer(
                        Math.min(PacketUtils.MAX_PAYLOAD, commandBytes.length - offset));
            }

        }

        if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
            PacketUtils.publishBigPacket(packet, sink, this::addAndGetSequenceId
                    , this.adjutant.alloc()::buffer, true);
        } else {
            PacketUtils.writePacketHeader(packet, addAndGetSequenceId());
            sink.next(packet);
        }

        sink.complete();

    }

    private void sendPrepareCommand(StatementCommandWriter commandWriter, List<BindValue> parameterGroup) {
        // result sequence_id
        this.updateSequenceId(-1);
        this.packetPublisher = commandWriter.writeCommand(parameterGroup);
    }


    private boolean skipTextResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        ResultSetReader dirtyResultSetReader = this.dirtyResultSetReader;
        if (dirtyResultSetReader == null) {
            dirtyResultSetReader = createDirtyResultReader();
            this.dirtyResultSetReader = dirtyResultSetReader;
        }
        boolean resultEnd;
        resultEnd = dirtyResultSetReader.read(cumulateBuffer, serverStatusConsumer);
        if (resultEnd) {
            this.dirtyResultSetReader = null;
        }
        return resultEnd;
    }

    private ResultSetReader createDirtyResultReader() {
        return ResultSetReaderBuilder.builder()
                .rowSink(obtainDirtyRowSink())
                .adjutant(ComQueryTask.this.adjutant)
                .sequenceIdUpdater(ComQueryTask.this::updateSequenceId)

                .errorConsumer(ComQueryTask.this::addError)
                .errorJudger(ComQueryTask.this::hasError)
                .build(TextResultSetReader.class);
    }

    private ResultRowSink obtainDirtyRowSink() {
        ResultRowSink dirtyRowSink = this.dirtyRowSink;
        if (dirtyRowSink == null) {
            dirtyRowSink = new ResultRowSink() {
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
            this.dirtyRowSink = dirtyRowSink;
        }
        return dirtyRowSink;
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

        void nextUpdate(int resultSequenceId, ResultStates resultStates);

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

        private SingleQuerySink(FluxSink<ResultRow> sink, Consumer<ResultStates> statesConsumer) {
            assertSingleMode(this);

            this.sink = sink;
            this.statesConsumer = statesConsumer;
            this.resultSetReader = ResultSetReaderBuilder.builder()
                    .rowSink(this)
                    .adjutant(ComQueryTask.this.adjutant)
                    .sequenceIdUpdater(ComQueryTask.this::updateSequenceId)
                    .errorConsumer(ComQueryTask.this::addError)

                    .errorJudger(ComQueryTask.this::hasError)
                    .build(TextResultSetReader.class);
        }

        @Override
        public String toString() {
            return SingleUpdateSink.class.getSimpleName();
        }

        @Override
        public void error(final JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public void next(ResultRow resultRow) {
            this.sink.next(resultRow);
        }

        @Override
        public boolean isCancelled() {
            return ComQueryTask.this.hasError() || this.sink.isCancelled();
        }

        @Override
        public void accept(ResultStates resultStates) {
            if (this.resultSetReader != null) {
                throw new IllegalStateException(String.format("%s.resultStates isn't null,reject update.", this));
            }
            this.resultStates = resultStates;
            ComQueryTask.this.updateLastResultStates(1, resultStates);
        }

        @Override
        public void nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            if (resultSequenceId == 1) {
                addError(new ErrorSubscribeException(ResultType.QUERY, ResultType.UPDATE));
            } else {
                addMultiStatementException();
            }
            this.resultEnd = true;
        }

        @Override
        public boolean readTextResultSet(int resultSequenceId, final ByteBuf cumulateBuffer
                , final Consumer<Object> serverStatusConsumer) {
            final boolean resultEnd;
            if (resultSequenceId == 1) {
                resultEnd = this.resultSetReader.read(cumulateBuffer, serverStatusConsumer);
                if (this.resultStates == null) {
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
        public void nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            if (resultSequenceId == 1) {
                this.resultStates = resultStates;
            } else {
                addMultiStatementException();
            }
            this.resultEnd = true;
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
            resultEnd = skipTextResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultEnd) {
                this.resultEnd = true;
            }
            return resultEnd;
        }

        @Override
        public boolean skipRestResults() {
            return this.resultEnd;
        }
    }

    private final class SingleStatementBatchUpdate implements DownstreamSink {

        private final List<String> sqlList;

        private final FluxSink<ResultStates> sink;

        private int currentSequenceId = 1;

        private SingleStatementBatchUpdate(List<String> sqlList, FluxSink<ResultStates> sink) {
            this.sqlList = sqlList;
            this.sink = sink;
        }

        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public void nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            final int batchCount = this.sqlList.size();
            if (resultSequenceId > batchCount) {
                addMultiStatementException();
            } else if (this.currentSequenceId == resultSequenceId) {
                this.sink.next(resultStates);
                if (resultSequenceId < batchCount) {
                    ComQueryTask.this.sendStaticCommand(this.sqlList.get(this.currentSequenceId++));
                }
            } else if (!hasError()) {
                throw new IllegalStateException(String.format(
                        "resultSequenceId[%s] and this.currentSequenceId[%s] not match."
                        , resultSequenceId, this.currentSequenceId));
            }
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
                this.currentSequenceId = this.sqlList.size();
            }
            return resultEnd;
        }

        @Override
        public void complete() {
            this.sink.complete();
        }

        @Override
        public boolean skipRestResults() {
            return this.currentSequenceId == this.sqlList.size();
        }
    }

    private final class SingleStatementPrepareBatchUpdateSink implements DownstreamSink {

        private final List<List<BindValue>> parameterGroupList;

        private final FluxSink<ResultStates> sink;

        private final StatementCommandWriter commandWriter;

        private int currentSequenceId = 1;


        private SingleStatementPrepareBatchUpdateSink(SQLStatement sqlStatement, List<List<BindValue>> parameterGroupList
                , FluxSink<ResultStates> sink) {
            assertSingleMode(this);

            this.parameterGroupList = parameterGroupList;
            this.sink = sink;
            this.commandWriter = new ComQueryCommandWriter(sqlStatement, ComQueryTask.this::addAndGetSequenceId
                    , ComQueryTask.this.adjutant);
        }

        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public void complete() {
            this.sink.complete();
        }

        @Override
        public void nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            final int batchCount = this.parameterGroupList.size();
            if (resultSequenceId > batchCount) {
                addMultiStatementException();
            } else if (this.currentSequenceId == resultSequenceId) {
                this.sink.next(resultStates);
                if (resultSequenceId < batchCount) {
                    // result sequence_id
                    ComQueryTask.this.sendPrepareCommand(this.commandWriter
                            , this.parameterGroupList.get(this.currentSequenceId++));

                }
            } else if (!hasError()) {
                throw new IllegalStateException(String.format(
                        "resultSequenceId[%s] and this.currentSequenceId[%s] not match."
                        , resultSequenceId, this.currentSequenceId));
            }
        }

        @Override
        public boolean readTextResultSet(final int resultSequenceId, final ByteBuf cumulateBuffer
                , final Consumer<Object> serverStatusConsumer) {
            if (!hasException(ErrorSubscribeException.class)) {
                addError(new ErrorSubscribeException(ResultType.BATCH_UPDATE, ResultType.QUERY));
            }

            boolean resultEnd;
            resultEnd = ComQueryTask.this.skipTextResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultEnd) {
                this.currentSequenceId = this.parameterGroupList.size();
            }
            return resultEnd;
        }

        @Override
        public boolean skipRestResults() {
            return this.currentSequenceId == this.parameterGroupList.size();
        }

    }

    private final class MultiStatementBatchUpdateSink implements DownstreamSink {

        private final FluxSink<ResultStates> sink;

        private final int batchCount;

        private int currentSequenceId = 1;

        private MultiStatementBatchUpdateSink(FluxSink<ResultStates> sink, int batchCount) {
            this.sink = sink;
            this.batchCount = batchCount;
        }

        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public void nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            if (resultSequenceId == this.currentSequenceId) {
                this.sink.next(resultStates);
                this.currentSequenceId++;
            } else if (!hasError()) {
                throw new IllegalStateException(String.format(
                        "resultSequenceId[%s] and this.currentSequenceId[%s] not match."
                        , resultSequenceId, this.currentSequenceId));
            }

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
                this.currentSequenceId = this.batchCount;
            }
            return resultEnd;
        }

        @Override
        public void complete() {
            this.sink.complete();
        }

        @Override
        public boolean skipRestResults() {
            return this.currentSequenceId == this.batchCount;
        }
    }

    private final class MultiStmtSink implements DownstreamSink {

        private final MultiResultsSink sink;

        private final int statementCount;

        private ResultSetReader resultSetReader;

        private QueryResultRowSink resultRowSink;

        private int currentSequenceId = 1;

        private MultiStmtSink(MultiResultsSink sink, int statementCount) {
            if (ComQueryTask.this.mode != Mode.MULTI_STMT && ComQueryTask.this.mode != Mode.TEMP_MULTI) {
                throw new IllegalStateException(String.format("%s mode[%s] error,reject create instance."
                        , this, ComQueryTask.this.mode));
            }
            this.sink = sink;
            this.statementCount = statementCount;
        }

        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public void complete() {
            if (this.currentSequenceId <= this.statementCount || ComQueryTask.this.hasMoreResults()) {
                throw new IllegalStateException(String.format(
                        "%s has more results,current sequenceId[%s],expect result count[%s], reject complete."
                        , this, this.currentSequenceId, this.statementCount));
            }
        }

        @Override
        public void nextUpdate(final int resultSequenceId, final ResultStates resultStates) {
            if (resultSequenceId == this.currentSequenceId) {
                this.sink.nextUpdate(resultStates);
                this.currentSequenceId++;
                if (resultSequenceId == this.statementCount && resultStates.hasMoreResults()) {
                    throw new IllegalStateException(String.format(
                            "%s has more results,current sequenceId[%s],expect result count[%s]."
                            , this, this.currentSequenceId, this.statementCount));
                }
            } else if (!hasError()) {
                throw new IllegalStateException(String.format(
                        "resultSequenceId[%s] and this.currentSequenceId[%s] not match."
                        , resultSequenceId, this.currentSequenceId));
            }
        }

        @Override
        public boolean readTextResultSet(final int resultSequenceId, ByteBuf cumulateBuffer
                , final Consumer<Object> serverStatusConsumer) {
            boolean resultEnd;
            if (resultSequenceId == this.currentSequenceId) {
                ResultSetReader resultSetReader = this.resultSetReader;
                if (resultSetReader == null) {
                    final QueryResultRowSink resultRowSink = new QueryResultRowSink(this.sink.nextQuery());
                    this.resultRowSink = resultRowSink;
                    resultSetReader = ResultSetReaderBuilder.builder()

                            .rowSink(resultRowSink)
                            .adjutant(ComQueryTask.this.adjutant)
                            .sequenceIdUpdater(ComQueryTask.this::updateSequenceId)
                            .errorConsumer(ComQueryTask.this::addError)

                            .errorJudger(ComQueryTask.this::hasError)
                            .build(TextResultSetReader.class);
                    this.resultSetReader = resultSetReader;
                }

                resultEnd = resultSetReader.read(cumulateBuffer, serverStatusConsumer);
                if (resultEnd) {
                    final QueryResultRowSink resultRowSink = this.resultRowSink;
                    if (!hasError()) {
                        resultRowSink.sink.complete();
                    }
                    this.resultRowSink = null;
                    this.resultSetReader = null;
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
            return this.currentSequenceId > this.statementCount;
        }
    }

    private final class QueryResultRowSink implements ResultRowSink {

        private final FluxSink<ResultRow> sink;

        private final QuerySink querySink;

        public QueryResultRowSink(QuerySink querySink) {
            this.sink = querySink.getSink();
            this.querySink = querySink;
        }

        @Override
        public void next(ResultRow resultRow) {
            this.sink.next(resultRow);
        }

        @Override
        public boolean isCancelled() {
            return ComQueryTask.this.hasError() || this.sink.isCancelled();
        }

        @Override
        public void accept(ResultStates resultStates) {
            this.querySink.acceptStatus(resultStates);
        }
    }


    /**
     * invoke this method after invoke {@link PacketUtils#hasOnePacket(ByteBuf)}.
     *
     * @see #decode(ByteBuf, Consumer)
     */
    static ComQueryResponse detectComQueryResponseType(final ByteBuf cumulateBuf, final int negotiatedCapability) {
        int readerIndex = cumulateBuf.readerIndex();
        final int payloadLength = PacketUtils.getInt3(cumulateBuf, readerIndex);
        // skip header
        readerIndex += PacketUtils.HEADER_SIZE;
        ComQueryResponse responseType;
        final boolean metadata = (negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;

        switch (PacketUtils.getInt1(cumulateBuf, readerIndex++)) {
            case 0:
                if (metadata && PacketUtils.obtainLenEncIntByteCount(cumulateBuf, readerIndex) + 1 == payloadLength) {
                    responseType = ComQueryResponse.TEXT_RESULT;
                } else {
                    responseType = ComQueryResponse.OK;
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
        LOCAL_INFILE_REQUEST
    }


    private enum Mode {
        SINGLE_STMT,
        MULTI_STMT,
        TEMP_MULTI
    }


}
