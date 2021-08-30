package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.util.PgArrays;
import io.jdbd.vendor.result.FluxResultSink;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.StringTokenizer;
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
abstract class AbstractStmtTask extends PgTask implements StmtTask {

    final Logger log = LoggerFactory.getLogger(getClass());

    private static final Set<String> UPDATE_COMMANDS = PgArrays.asUnmodifiableSet("INSERT", "UPDATE", "DELETE");

    final FluxResultSink sink;

    final Stmt stmt;

    private int resultIndex = 0;

    private boolean downstreamCanceled;

    private CopyOperationHandler copyOperationHandler;

    AbstractStmtTask(TaskAdjutant adjutant, FluxResultSink sink, @Nullable Stmt stmt) {
        super(adjutant, sink::error);
        this.sink = sink;
        this.stmt = stmt;
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
    public final boolean readResultStateWithReturning(ByteBuf cumulateBuffer, boolean moreFetch
            , Supplier<Integer> resultIndexes) {
        return readCommandComplete(cumulateBuffer, true, moreFetch, resultIndexes);
    }

    @Override
    public final boolean isCanceled() {
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

    abstract void internalToString(StringBuilder builder);

    /**
     * @return true: read CommandComplete message end , false : more cumulate.
     */
    final boolean readResultStateWithoutReturning(ByteBuf cumulateBuffer) {
        return readCommandComplete(cumulateBuffer, false, false, this::getAndIncrementResultIndex);
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
            states = PgResultStates.empty(getAndIncrementResultIndex(), moreResult);
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
     * @return true: read CommandComplete message end , false : more cumulate.
     * @see #readResultStateWithReturning(ByteBuf, boolean, Supplier)
     * @see #readResultStateWithoutReturning(ByteBuf)
     */
    private boolean readCommandComplete(final ByteBuf cumulateBuffer, final boolean hasReturningColumn
            , final boolean moreFetch, Supplier<Integer> resultIndexes) {
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

        final ResultStateParams params = new ResultStateParams();
        final StringTokenizer tokenizer = new StringTokenizer(commandTag, " ");
        final String command;
        switch (tokenizer.countTokens()) {
            case 1:
                command = tokenizer.nextToken();
                break;
            case 2: {
                command = tokenizer.nextToken();
                params.affectedRows = Long.parseLong(tokenizer.nextToken());
            }
            break;
            case 3: {
                command = tokenizer.nextToken();
                params.insertId = Long.parseLong(tokenizer.nextToken());
                params.affectedRows = Long.parseLong(tokenizer.nextToken());
            }
            break;
            default:
                String m = String.format("Server response CommandComplete command tag[%s] format error."
                        , commandTag);
                throw new PgJdbdException(m);
        }
        log.trace("Read CommandComplete message command tag[{}],command[{}]", commandTag, command);

        params.moreResult = status == ResultSetStatus.MORE_RESULT;
        params.hasReturningColumn = hasReturningColumn;
        params.moreFetch = moreFetch;
        if (cumulateBuffer.getInt(nextMsgIndex) == Messages.N) {
            // next is warning NoticeResponse
            params.noticeMessage = NoticeMessage.read(cumulateBuffer, clientCharset);
        }
        params.resultIndex = resultIndexes.get();

        if (!this.isCanceled()) {
            this.sink.next(PgResultStates.create(params));
        }
        return true;
    }


}
