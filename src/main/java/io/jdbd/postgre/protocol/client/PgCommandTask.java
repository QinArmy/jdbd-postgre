package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.util.PgArrays;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.stmt.*;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.IntSupplier;

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


    private static final String INSERT = "INSERT";

    private static final List<String> UPDATE_COMMANDS = PgArrays.unmodifiableListOf(INSERT, "UPDATE", "DELETE");

    private static final String SELECT = "SELECT";

    private static final String SET = "SET";


    private final ResultSetReader resultSetReader;

    private boolean readResultSetPhase;

    private int resultIndex = 0;

    private boolean downstreamCanceled;

    private CopyOperationHandler copyOperationHandler;

    private Set<Integer> unknownTypeOidSet;


    /**
     * <p>
     * This constructor for : <ul>
     * <li>{@link SimpleQueryTask}</li>
     * <li>{@link ExtendedQueryTask}</li>
     * </ul>
     * </p>
     */
    PgCommandTask(TaskAdjutant adjutant, ResultSink sink) {
        super(adjutant, sink::error);
        this.resultSetReader = PgResultSetReader.create(this);
    }


    /**
     * <p>
     * This constructor for : <ul>
     * <li>{@link PgCursorTask}</li>
     * </ul>
     * </p>
     * <p>
     * If use this constructor ,then must override {@link #emitError(Throwable)}
     * </p>
     */
    PgCommandTask(TaskAdjutant adjutant) {
        super(adjutant);
        assert this instanceof PgCursorTask;
        this.resultSetReader = PgResultSetReader.create(this);
    }

    @Override
    public final TaskAdjutant adjutant() {
        return this.adjutant;
    }

    @Override
    public final int nextResultNo() {
        return this.resultIndex++;
    }

    @Override
    public final void addErrorToTask(Throwable error) {
        addError(error);
    }

    @Override
    public final boolean readResultStateOfQuery(ByteBuf cumulateBuffer, IntSupplier resultIndexes) {
        return readCommandComplete(cumulateBuffer, true, resultIndexes);
    }

    @Override
    public final boolean isCancelled() {
        final Logger logger = getLog();

        final boolean isCanceled;
        if (this.downstreamCanceled || hasError()) {
            isCanceled = true;
        } else if (this.isDownstreamCanceled()) {
            logger.trace("Downstream cancel subscribe.");
            isCanceled = true;
            this.downstreamCanceled = true;
        } else {
            isCanceled = false;
        }
        return isCanceled;
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

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Message Formats</a>
     */
    final boolean readExecuteResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {

        final Charset clientCharset = this.adjutant.clientCharset();
        final Logger logger = getLog();

        boolean taskEnd = false, continueRead = Messages.hasOneMessage(cumulateBuffer);
        for (int msgStartIndex, msgType; continueRead; ) {

            if (this.readResultSetPhase) {
                if (this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
                    this.readResultSetPhase = false;
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                } else {
                    continueRead = false;
                }
                continue;
            }

            msgStartIndex = cumulateBuffer.readerIndex();
            msgType = cumulateBuffer.getByte(msgStartIndex);

            switch (msgType) {
                case Messages.E: {// ErrorResponse message
                    addError(PgServerException.read(cumulateBuffer, clientCharset));
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
                    logger.debug("receive NoData message,sql no ResultSet response.");
                }
                break;
                case Messages.s: {// PortalSuspended message
                    Messages.skipOneMessage(cumulateBuffer); //TODO fix me
                    logger.debug("receive PortalSuspended message,client timeout.");
                    handleClientTimeout();
                }
                break;
                case Messages.Z: {// ReadyForQuery message
                    serverStatusConsumer.accept(TxStatus.read(cumulateBuffer));
                    taskEnd = true;
                    continueRead = false;
                    logger.trace("Simple query command end,read optional notice.");
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

    abstract Logger getLog();

    abstract void internalToString(StringBuilder builder);


    abstract boolean isDownstreamCanceled();

    /**
     * @return true: task end.
     */
    abstract boolean handlePrepareResponse(List<DataType> paramTypeList, @Nullable PgRowMeta rowMeta);

    abstract boolean handleClientTimeout();

    abstract Stmt getStmt();

    boolean handleSelectCommand(long rowCount) {
        // sub class override.
        return false;
    }


    /**
     * @return true: read CommandComplete message end , false : more cumulate.
     */
    final boolean readResultStateWithoutReturning(ByteBuf cumulateBuffer) {
        return readCommandComplete(cumulateBuffer, false, this::nextResultNo);
    }

    /**
     * @return true: read EmptyQueryResponse message end , false : more cumulate.
     */
    final boolean readEmptyQuery(final ByteBuf cumulateBuffer) {
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
            states = PgResultStates.empty(nextResultNo(), moreResult
                    , this.adjutant.server().serverVersion());
            this.next(states);
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
        addError(new UnExpectedMessageException(msg));
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
     * ,and invoke {@link #handlePrepareResponse(List, PgRowMeta).}
     * </p>
     *
     * @return true : task end
     * @see #readExecuteResponse(ByteBuf, Consumer)
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ParameterDescription (B)</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription (B)</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">NoData</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ReadyForQuery</a>
     */
    private boolean readPrepareResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        final List<DataType> paramTypeList;
        paramTypeList = readParameterDescription(cumulateBuffer);

        final PgRowMeta rowMeta;
        switch (cumulateBuffer.getByte(cumulateBuffer.readerIndex())) {
            case Messages.T: // RowDescription message
                rowMeta = PgRowMeta.readForPrepare(cumulateBuffer, this.adjutant);
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
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ParameterDescription (B)</a>
     */
    private List<DataType> readParameterDescription(final ByteBuf cumulateBuffer) {
        final int msgStartIndex = cumulateBuffer.readerIndex();
        if (cumulateBuffer.readByte() != Messages.t) {
            throw new IllegalArgumentException("Non ParameterDescription message");
        }
        final int length = cumulateBuffer.readInt();
        final int count = cumulateBuffer.readShort();

        final IntFunction<DataType> typeFunc = this.adjutant.oidToDataTypeFunc();
        final List<DataType> paramTypeList;
        DataType dataType;
        int oid;
        switch (count) {
            case 0:
                paramTypeList = Collections.emptyList();
                break;
            case 1: {
                oid = cumulateBuffer.readInt();
                dataType = PgType.from(oid);
                if (dataType == PgType.UNSPECIFIED) {
                    dataType = typeFunc.apply(oid);
                }
                if (dataType == PgType.UNSPECIFIED) {
                    addUnknownTypeOid(oid);
                }
                paramTypeList = Collections.singletonList(dataType);
            }
            break;
            default: {
                paramTypeList = PgCollections.arrayList(count);
                for (int i = 0; i < count; i++) {
                    oid = cumulateBuffer.readInt();
                    dataType = PgType.from(oid);
                    if (dataType == PgType.UNSPECIFIED) {
                        dataType = typeFunc.apply(oid);
                    }
                    if (dataType == PgType.UNSPECIFIED) {
                        addUnknownTypeOid(oid);
                    }
                    paramTypeList.add(dataType);
                }
            }
        }
        cumulateBuffer.readerIndex(msgStartIndex + 1 + length); // avoid tail filler
        return paramTypeList;
    }


    /**
     * @return true: read CommandComplete message end , false : more cumulate.
     * @see #readResultStateOfQuery(ByteBuf, IntSupplier)
     * @see #readResultStateWithoutReturning(ByteBuf)
     */
    private boolean readCommandComplete(final ByteBuf cumulateBuffer, final boolean hasColumn,
                                        final IntSupplier resultIndexFunc) {
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
        final int resultIndex = resultIndexFunc.getAsInt();
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
            m = String.format("Postgre server CommandComplete message error,can't read command from command tag[%s].",
                    commandTag);
            addError(new JdbdException(m));
        }
        getLog().trace("Read CommandComplete message command tag[{}],command[{}]", commandTag, command);

        params.moreResult = status == ResultSetStatus.MORE_RESULT;
        params.hasColumn = hasColumn;
        if (cumulateBuffer.getInt(nextMsgIndex) == Messages.N) {
            // next is warning NoticeResponse
            params.noticeMessage = NoticeMessage.read(cumulateBuffer, clientCharset);
        }


        if (!this.isCancelled()) {
            this.next(PgResultStates.create(params));
        }
        return true;
    }

    private void handleSetCommand(final int resultIndex) {
        final Stmt stmt = this.getStmt();
        try {
            final PgParser parser = this.adjutant;
            final String sql;
            if (stmt instanceof SingleStmt) {
                final List<String> sqlList = parser.separateMultiStmt(((SingleStmt) stmt).getSql());
                sql = sqlList.get(resultIndex);
            } else if (stmt instanceof ParamMultiStmt) {
                final ParamStmt bindStmt = ((ParamMultiStmt) stmt).getStmtList().get(resultIndex);
                sql = bindStmt.getSql();
            } else if (stmt instanceof StaticBatchStmt) {
                sql = ((StaticBatchStmt) stmt).getSqlGroup().get(resultIndex);
            } else {
                sql = null;
                getLog().debug("Unknown Stmt[{}]", stmt);
            }
            if (sql != null) {
                this.adjutant.appendSetCommandParameter(parser.parseSetParameter(sql));
            }
        } catch (Throwable e) {
            this.addError(e);
        }

    }


    private void addUnknownTypeOid(int oid) {
        Set<Integer> set = this.unknownTypeOidSet;
        if (set == null) {
            this.unknownTypeOidSet = set = PgCollections.hashSet();
        }
        set.add(oid);
    }


}
