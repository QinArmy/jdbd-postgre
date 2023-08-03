package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.util.PgArrays;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.stmt.SingleStmt;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * <p>
 * This class is base class of :
 *     <ul>
 *         <li>{@link SimpleQueryTask}</li>
 *         <li>{@link ExtendedQueryTask}</li>
 *     </ul>
 * </p>
 */
abstract class PgCommandTask extends PgTask implements StmtTask {

    final Logger log = LoggerFactory.getLogger(getClass());

    private static final String INSERT = "INSERT";

    private static final Set<String> UPDATE_COMMANDS = PgArrays.asUnmodifiableSet(INSERT, "UPDATE", "DELETE");

    private static final String SELECT = "SELECT";

    private static final String SET = "SET";

    final ResultSink sink;

    final Stmt stmt;

    private final ResultSetReader resultSetReader;

    private boolean readResultSetPhase;

    private int resultIndex = 0;

    private boolean downstreamCanceled;

    private CopyOperationHandler copyOperationHandler;


    PgCommandTask(TaskAdjutant adjutant, ResultSink sink, Stmt stmt) {
        super(adjutant, sink::error);
        this.sink = sink;
        this.stmt = stmt;
        this.resultSetReader = PgResultSetReader.create(this);
    }

    @Override
    public final TaskAdjutant adjutant() {
        return this.adjutant;
    }

    @Override
    public final int getAndIncrementResultIndex() {
        return this.resultIndex++;
    }

    @Override
    public final void addErrorToTask(Throwable error) {
        addError(error);
    }

    @Override
    public final boolean readResultStateWithReturning(ByteBuf cumulateBuffer, Supplier<Integer> resultIndexes) {
        return readCommandComplete(cumulateBuffer, true, resultIndexes);
    }

    @Override
    public final boolean isCancelled() {
        final boolean isCanceled;
        if (this.downstreamCanceled || hasError()) {
            isCanceled = true;
        } else if (this.sink.isCancelled()) {
            log.trace("Downstream cancel subscribe.");
            isCanceled = true;
            this.downstreamCanceled = true;
        } else {
            isCanceled = false;
        }
        log.trace("Read command response,isCanceled:{}", isCanceled);
        return isCanceled;
    }

    @Override
    public final void next(final Result result) {
        this.sink.next(result);
    }

    @Override
    public final String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append(getClass().getSimpleName())
                .append("[resultIndex:")
                .append(this.resultIndex)
                .append(",downstreamCanceled:")
                .append(this.downstreamCanceled)
                .append(",copyOperationHandler:")
                .append(this.copyOperationHandler)
                .append(",hasError:")
                .append(hasError());

        internalToString(builder);
        builder.append("]");
        return builder.toString();
    }

    @Override
    protected final boolean skipPacketsOnError(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        cumulateBuffer.readerIndex(cumulateBuffer.writerIndex());
        return true;
    }

    final boolean readExecuteResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        boolean taskEnd = false, continueRead = Messages.hasOneMessage(cumulateBuffer);

        final Charset clientCharset = this.adjutant.clientCharset();

        while (continueRead) {

            if (this.readResultSetPhase) {
                if (this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
                    this.readResultSetPhase = false;
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                } else {
                    continueRead = false;
                }
                continue;
            }

            final int msgStartIndex = cumulateBuffer.readerIndex();
            final int msgType = cumulateBuffer.getByte(msgStartIndex);

            switch (msgType) {
                case Messages.E: {// ErrorResponse message
                    ErrorMessage error = ErrorMessage.read(cumulateBuffer, clientCharset);
                    addError(PgExceptions.createErrorException(error));
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case Messages.C: {// CommandComplete message,if return rows, CommandComplete message is read by io.jdbd.postgre.protocol.client.ResultSetReader.read()
                    if (readResultStateWithoutReturning(cumulateBuffer)) {
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }
                }
                break;
                case Messages.I: {// EmptyQueryResponse message
                    continueRead = readEmptyQuery(cumulateBuffer) && Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case Messages.S: {// ParameterStatus message
                    serverStatusConsumer.accept(Messages.readParameterStatus(cumulateBuffer, clientCharset));
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case Messages.t: { // ParameterDescription
                    if (!Messages.canReadDescribeResponse(cumulateBuffer)) {// cumulate util exists ReadyForQuery
                        // need cumulating for  RowDescription message or NoData message
                        continueRead = false;
                        continue;
                    }
                    taskEnd = readPrepareResponse(cumulateBuffer, serverStatusConsumer);
                    continueRead = !taskEnd && Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case Messages.T: {// RowDescription message, must after Messages.t
                    this.readResultSetPhase = true;
                    if (this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
                        this.readResultSetPhase = false;
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }
                }
                break;
                case Messages.G: {// CopyInResponse message
                    handleCopyInResponse(cumulateBuffer);
                    continueRead = false;
                }
                break;
                case Messages.H: { // CopyOutResponse message
                    if (handleCopyOutResponse(cumulateBuffer)) {
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }
                }
                break;
                case Messages.d: // CopyData message
                case Messages.c:// CopyDone message
                case Messages.f: {// CopyFail message
                    if (handleCopyOutData(cumulateBuffer)) {
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }
                }
                break;
                case Messages.CHAR_ONE:// ParseComplete message
                case Messages.CHAR_TWO:// BindComplete message
                case Messages.A: { // NotificationResponse
                    //TODO complete LISTEN command
                    Messages.skipOneMessage(cumulateBuffer);
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case Messages.n: {// NoData message
                    Messages.skipOneMessage(cumulateBuffer);
                    // here , sql is DML
                    log.debug("receive NoData message,sql no ResultSet response.");
                }
                break;
                case Messages.s: {// PortalSuspended message
                    Messages.skipOneMessage(cumulateBuffer);
                    log.debug("receive PortalSuspended message,client timeout.");
                    handleClientTimeout();
                }
                break;
                case Messages.Z: {// ReadyForQuery message,Messages.Z must last,because ParameterDescription can follow by ReadyForQuery
                    final TxStatus txStatus = TxStatus.read(cumulateBuffer);
                    serverStatusConsumer.accept(txStatus);
                    taskEnd = true;
                    continueRead = false;
                    log.trace("Simple query command end,read optional notice.");
                    readNoticeAfterReadyForQuery(cumulateBuffer, serverStatusConsumer);
                }
                break;
                default: {
                    handleUnexpectedMessage(cumulateBuffer);
                }

            }


        }
        return taskEnd;
    }

    abstract void internalToString(StringBuilder builder);

    /**
     * @return true: task end.
     */
    abstract boolean handlePrepareResponse(List<PgType> paramTypeList, @Nullable ResultRowMeta rowMeta);

    abstract boolean handleClientTimeout();

    boolean handleSelectCommand(long rowCount) {
        // sub class override.
        return false;
    }

    /**
     * @return true: read CommandComplete message end , false : more cumulate.
     */
    final boolean readResultStateWithoutReturning(ByteBuf cumulateBuffer) {
        return readCommandComplete(cumulateBuffer, false, this::getAndIncrementResultIndex);
    }

    /**
     * @return true: read EmptyQueryResponse message end , false : more cumulate.
     */
    final boolean readEmptyQuery(ByteBuf cumulateBuffer) {
        final int msgIndex = cumulateBuffer.readerIndex();
        if (cumulateBuffer.getByte(msgIndex) != Messages.I) {
            throw new IllegalArgumentException("Non EmptyQueryResponse message.");
        }
        final ResultSetStatus status = Messages.getResultSetStatus(cumulateBuffer);
        final boolean readEnd;
        if (status == ResultSetStatus.MORE_CUMULATE) {
            readEnd = false;
        } else {
            cumulateBuffer.readByte(); // skip message type byte.
            final int nextMsgIndex = msgIndex + 1 + cumulateBuffer.readInt();

            final PgResultStates states;
            final boolean moreResult = status == ResultSetStatus.MORE_RESULT;
            states = PgResultStates.empty(getAndIncrementResultIndex(), moreResult
                    , this.adjutant.server().serverVersion());
            this.sink.next(states);
            cumulateBuffer.readerIndex(nextMsgIndex); // avoid tail filler
            readEnd = true;
        }
        return readEnd;
    }

    final int getResultIndex() {
        return this.resultIndex;
    }


    /**
     * <p>
     * handle unexpected message and always throw {@link UnExpectedMessageException}
     * </p>
     *
     * @throws UnExpectedMessageException always
     */
    final void handleUnexpectedMessage(ByteBuf cumulateBuffer) throws UnExpectedMessageException {
        final char msgType = (char) cumulateBuffer.getByte(cumulateBuffer.readerIndex());
        Messages.skipOneMessage(cumulateBuffer);
        String msg = String.format("Server response unknown message type[%s]", msgType);
        throw new UnExpectedMessageException(msg);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyInResponse</a>
     */
    final void handleCopyInResponse(ByteBuf cumulateBuffer) {
        CopyOperationHandler copyOperationHandler = this.copyOperationHandler;
        if (copyOperationHandler == null) {
            copyOperationHandler = DefaultCopyOperationHandler.create(this, this::sendPacket);
            this.copyOperationHandler = copyOperationHandler;
        }
        copyOperationHandler.handleCopyInResponse(cumulateBuffer);

    }


    /**
     * @return true: copy out handle end.
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyOutResponse</a>
     */
    final boolean handleCopyOutResponse(ByteBuf cumulateBuffer) {
        CopyOperationHandler copyOperationHandler = this.copyOperationHandler;
        if (copyOperationHandler == null) {
            copyOperationHandler = DefaultCopyOperationHandler.create(this, this::sendPacket);
            this.copyOperationHandler = copyOperationHandler;
        }
        return copyOperationHandler.handleCopyOutResponse(cumulateBuffer);
    }


    /**
     * @return true: copy out end
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyData</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyDone</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyFail</a>
     */
    final boolean handleCopyOutData(ByteBuf cumulateBuffer) {
        final CopyOperationHandler copyOperationHandler = this.copyOperationHandler;
        if (copyOperationHandler == null) {
            throw new IllegalStateException("No CopyOperationHandler");
        }
        return copyOperationHandler.handleCopyOutData(cumulateBuffer);
    }

    /**
     * <p>
     * Read below messages:
     * <ol>
     *     <li>ParameterDescription</li>
     *     <li>RowDescription (or NoData)</li>
     *     <li>ReadyForQuery</li>
     * </ol>
     * ,and invoke {@link #handlePrepareResponse(List, ResultRowMeta).}
     * </p>
     *
     * @return true : task end
     * @see #readExecuteResponse(ByteBuf, Consumer)
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ParameterDescription</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">NoData</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ReadyForQuery</a>
     */
    private boolean readPrepareResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        final List<PgType> paramTypeList;
        paramTypeList = Messages.readParameterDescription(cumulateBuffer);

        final ResultRowMeta rowMeta;
        switch (cumulateBuffer.getByte(cumulateBuffer.readerIndex())) {
            case Messages.T: {// RowDescription message
                rowMeta = PgRowMeta.readForPrepare(cumulateBuffer, this.adjutant);
            }
            break;
            case Messages.n: {// NoData message
                Messages.skipOneMessage(cumulateBuffer);
                rowMeta = null;
            }
            break;
            default: {
                final char msgType = (char) cumulateBuffer.getByte(cumulateBuffer.readerIndex());
                String m = String.format("Unexpected message[%s] for read prepare response.", msgType);
                throw new UnExpectedMessageException(m);
            }

        }

        serverStatusConsumer.accept(TxStatus.read(cumulateBuffer)); // read ReadyForQuery message

        return handlePrepareResponse(paramTypeList, rowMeta);
    }


    /**
     * @return true: read CommandComplete message end , false : more cumulate.
     * @see #readResultStateWithReturning(ByteBuf, Supplier)
     * @see #readResultStateWithoutReturning(ByteBuf)
     */
    private boolean readCommandComplete(final ByteBuf cumulateBuffer, final boolean hasColumn
            , Supplier<Integer> resultIndexes) {
        final ResultSetStatus status = Messages.getResultSetStatus(cumulateBuffer);
        if (status == ResultSetStatus.MORE_CUMULATE) {
            return false;
        }
        final Charset clientCharset = this.adjutant.clientCharset();
        final int msgStartIndex = cumulateBuffer.readerIndex();
        if (cumulateBuffer.readByte() != Messages.C) {
            cumulateBuffer.readerIndex(msgStartIndex);
            throw new IllegalStateException("Non CommandComplete message.");
        }
        final int nextMsgIndex = msgStartIndex + 1 + cumulateBuffer.readInt();
        final String commandTag = Messages.readString(cumulateBuffer, clientCharset);
        cumulateBuffer.readerIndex(nextMsgIndex); // avoid tail filler

        final ResultStateParams params = new ResultStateParams(this.adjutant.server().serverVersion());
        final int resultIndex = resultIndexes.get();
        params.resultIndex = resultIndex;
        final String[] tagPart = commandTag.split("\\s");
        final String command;
        if (tagPart.length > 0) {
            command = tagPart[0].toUpperCase();
            if (SELECT.equals(command)) {
                final long rowCount = Long.parseLong(tagPart[1]);
                params.rowCount = rowCount;
                params.moreFetch = handleSelectCommand(rowCount);
            } else if (UPDATE_COMMANDS.contains(command)) {
                if (INSERT.equals(command)) {
                    params.insertId = Long.parseLong(tagPart[1]);
                    params.affectedRows = Long.parseLong(tagPart[2]);
                } else {
                    params.affectedRows = Long.parseLong(tagPart[1]);
                }
            } else if (SET.equals(command)) {
                handleSetCommand(resultIndex);
            }
        } else {
            command = "";
            String m;
            m = String.format("Postgre server CommandComplete message error,can't read command from command tag[%s]."
                    , commandTag);
            addError(new JdbdException(m));
        }
        log.trace("Read CommandComplete message command tag[{}],command[{}]", commandTag, command);

        params.moreResult = status == ResultSetStatus.MORE_RESULT;
        params.hasColumn = hasColumn;
        if (cumulateBuffer.getInt(nextMsgIndex) == Messages.N) {
            // next is warning NoticeResponse
            params.noticeMessage = NoticeMessage.read(cumulateBuffer, clientCharset);
        }


        if (!this.isCancelled()) {
            this.sink.next(PgResultStates.create(params));
        }
        return true;
    }

    private void handleSetCommand(final int resultIndex) {
        final Stmt stmt = this.stmt;
        try {
            final PgParser parser = this.adjutant.sqlParser();
            final String sql;
            if (stmt instanceof SingleStmt) {
                final List<String> sqlList = parser.separateMultiStmt(((SingleStmt) stmt).getSql());
                sql = sqlList.get(resultIndex);
            } else if (stmt instanceof BindMultiStmt) {
                final BindStmt bindStmt = ((BindMultiStmt) stmt).getStmtList().get(resultIndex);
                sql = bindStmt.getSql();
            } else if (stmt instanceof StaticBatchStmt) {
                sql = ((StaticBatchStmt) stmt).getSqlGroup().get(resultIndex);
            } else {
                sql = null;
                log.debug("Unknown Stmt[{}]", stmt);
            }
            if (sql != null) {
                this.adjutant.appendSetCommandParameter(parser.parseSetParameter(sql));
            }
        } catch (Throwable e) {
            this.addError(e);
        }

    }


}
