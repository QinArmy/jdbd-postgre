package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.postgre.stmt.MultiBindStmt;
import io.jdbd.postgre.syntax.CopyIn;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.util.PgArrays;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.vendor.result.FluxResultSink;
import io.jdbd.vendor.stmt.GroupStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
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

    private CopyIn cacheCopyIn;

    private List<String> singleSqlList;

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
    public final void addResultSetError(Throwable error) {
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
                .append(",cacheCopyIn:")
                .append(this.cacheCopyIn)
                .append(",singleSqlList size:");

        final List<String> singleSqlList = this.singleSqlList;
        if (singleSqlList == null) {
            builder.append("null");
        } else {
            builder.append(singleSqlList.size());
        }
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


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyInResponse</a>
     */
    final void handleCopyInResponse(ByteBuf cumulateBuffer) {
        final int msgStartIndex = cumulateBuffer.readerIndex();
        if (cumulateBuffer.readByte() != Messages.G) {
            throw new IllegalArgumentException("Non Copy-In message.");
        }
        cumulateBuffer.readerIndex(msgStartIndex + 1 + cumulateBuffer.readInt()); // skip CopyInResponse message,this message design too stupid ,it should return filename of STDIN,but not.

        final int resultIndex = this.resultIndex;
        Publisher<ByteBuf> publisher;
        try {
            final CopyIn copyIn = parseCopyIn(resultIndex);
            switch (copyIn.getMode()) {
                case FILE:
                    publisher = sendCopyInDataFromLocalPath(obtainPathFromCopyIn(resultIndex, copyIn));
                    break;
                case PROGRAM:
                case STDIN:
                    String msg = String.format("COPY FROM %s not supported by jdbd-postgre .", copyIn.getMode());
                    publisher = Mono.just(createCopyFailMessage(msg));
                    break;
                default:
                    throw PgExceptions.createUnknownEnumException(copyIn.getMode());
            }
        } catch (SQLException e) {
            String msg = String.format("Parse copy in sql error,%s", e.getMessage());
            publisher = Mono.just(createCopyFailMessage(msg));
        } catch (Throwable e) {
            String msg = String.format("Handle statement[index:%s] copy-in failure,message:%s"
                    , resultIndex, e.getMessage());
            publisher = Mono.just(createCopyFailMessage(msg));
        }
        this.packetPublisher = Objects.requireNonNull(publisher);
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyOutResponse</a>
     */
    final void handleCopyOutResponse(ByteBuf cumulateBuffer) {
        final int msgStartIndex = cumulateBuffer.readerIndex();
        if (cumulateBuffer.readByte() != Messages.H) {
            throw new IllegalArgumentException("Non Copy-out message.");
        }
        final int nextMsgIndex = msgStartIndex + 1 + cumulateBuffer.readInt();

        cumulateBuffer.readerIndex(nextMsgIndex);


    }

    /**
     * @see #handleCopyInResponse(ByteBuf)
     */
    private CopyIn parseCopyIn(final int resultIndex) throws Exception {
        final PgParser parser = this.adjutant.sqlParser();
        final Stmt stmt = this.stmt;
        final CopyIn copyIn;
        if (stmt instanceof StaticStmt) {
            List<String> singleSqlList = this.singleSqlList;
            if (singleSqlList == null) {
                singleSqlList = parser.separateMultiStmt(((StaticStmt) stmt).getSql());
                this.singleSqlList = singleSqlList;
            }
            if (resultIndex >= singleSqlList.size()) {
                // here 1. bug ; 2. postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                throw new IllegalStateException(String.format("IllegalState can't found COPY command,%s", this));
            }
            copyIn = parser.parseCopyIn(singleSqlList.get(resultIndex));
        } else if (stmt instanceof GroupStmt) {
            final List<String> sqlGroup = ((GroupStmt) stmt).getSqlGroup();
            if (resultIndex >= sqlGroup.size()) {
                // here 1. bug ; 2. postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                throw new IllegalStateException(String.format("IllegalState can't found COPY command,%s", this));
            }
            copyIn = parser.parseCopyIn(sqlGroup.get(resultIndex));
        } else if (stmt instanceof BatchBindStmt) {
            CopyIn cacheCopyIn = this.cacheCopyIn;
            if (cacheCopyIn == null) {
                cacheCopyIn = parser.parseCopyIn(((BatchBindStmt) stmt).getSql());
                this.cacheCopyIn = cacheCopyIn;
            }
            final List<List<BindValue>> groupList = ((BatchBindStmt) stmt).getGroupList();
            if (resultIndex >= groupList.size()) {
                // here 1. bug ; 2. postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                throw new IllegalStateException(String.format("IllegalState can't found COPY command,%s", this));
            }
            copyIn = cacheCopyIn;
        } else if (stmt instanceof BindableStmt) {
            if (resultIndex != 0) {
                // here  postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                String m;
                m = String.format("Postgre response %s CommendComplete message,but expect one.", resultIndex + 1);
                throw new PgJdbdException(m);
            }
            copyIn = parser.parseCopyIn(((BindableStmt) stmt).getSql());
        } else if (stmt instanceof MultiBindStmt) {
            final List<BindableStmt> stmtGroup = ((MultiBindStmt) stmt).getStmtGroup();
            if (resultIndex >= stmtGroup.size()) {
                // here 1. bug ; 2. postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                throw new IllegalStateException(String.format("IllegalState can't found COPY command,%s", this));
            }
            copyIn = parser.parseCopyIn(stmtGroup.get(resultIndex).getSql());
        } else {
            String m = String.format("Unknown %s type[%s]", Stmt.class.getName(), stmt.getClass().getName());
            throw new IllegalStateException(m);
        }
        return copyIn;
    }

    /**
     * @see #handleCopyInResponse(ByteBuf)
     */
    private Path obtainPathFromCopyIn(final int resultIndex, final CopyIn copyIn) {
        final int bindIndex = copyIn.getBindIndex();
        final Stmt stmt = this.stmt;
        final Path path;
        if (bindIndex < 0) {
            path = copyIn.getPath();
        } else if (stmt instanceof BindableStmt) {
            path = obtainPathFromParamGroup(((BindableStmt) stmt).getParamGroup(), resultIndex, bindIndex);
        } else if (stmt instanceof BatchBindStmt) {
            final List<List<BindValue>> groupList = ((BatchBindStmt) stmt).getGroupList();
            if (resultIndex >= groupList.size()) {
                // here  bug
                throw new IllegalStateException(String.format("IllegalState can't found COPY command,%s", this));
            }
            path = obtainPathFromParamGroup(groupList.get(resultIndex), resultIndex, bindIndex);
        } else if (stmt instanceof MultiBindStmt) {
            final List<BindableStmt> stmtGroup = ((MultiBindStmt) stmt).getStmtGroup();
            if (resultIndex >= stmtGroup.size()) {
                // here  bug
                throw new IllegalStateException(String.format("IllegalState can't found COPY command,%s", this));
            }
            final List<BindValue> valueList = stmtGroup.get(resultIndex).getParamGroup();
            path = obtainPathFromParamGroup(valueList, resultIndex, bindIndex);
        } else {
            throw new IllegalStateException(String.format("Can't obtain path from Stmt[%s].", stmt));
        }
        return path;
    }

    /**
     * @see #obtainPathFromCopyIn(int, CopyIn)
     */
    private Path obtainPathFromParamGroup(final List<BindValue> valueList, final int resultIndex, final int bindIndex) {
        if (bindIndex >= valueList.size()) {
            //here  bug
            String m;
            m = String.format("IllegalState can't obtain 'filename' for COPY operation,Stmt[index:%s],bind[index:%s]"
                    , resultIndex, bindIndex);
            throw new IllegalStateException(m);
        }
        final Object value = valueList.get(bindIndex).getValue();
        if (!(value instanceof String)) {
            String className = value == null ? null : value.getClass().getName();
            String m = String.format("Statement[index:%s] can't obtain local file from bind type[%s],bind[index:%s]"
                    , resultIndex, className, bindIndex);
            throw new IllegalStateException(m);
        }
        return Paths.get((String) value);
    }

    /**
     * @see #handleCopyInResponse(ByteBuf)
     */
    private Publisher<ByteBuf> sendCopyInDataFromLocalPath(final Path path) {
        final String errorMsg;
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            errorMsg = String.format("filename[%s] not exists.", path.toAbsolutePath());
        } else if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            errorMsg = String.format("filename[%s] is directory.", path.toAbsolutePath());
        } else if (!Files.isReadable(path)) {
            errorMsg = String.format("filename[%s] isn't readable.", path.toAbsolutePath());
        } else {
            errorMsg = null;
        }

        final Publisher<ByteBuf> publisher;
        if (errorMsg == null) {
            publisher = Flux.create(sink -> executeSendCopyInDataFromLocalPath(path, sink));
        } else {
            publisher = Mono.just(createCopyFailMessage(errorMsg));
        }
        return publisher;
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyFail</a>
     */
    final ByteBuf createCopyFailMessage(String errorInfo) {
        final byte[] bytes = errorInfo.getBytes(this.adjutant.clientCharset());
        final ByteBuf message = this.adjutant.allocator().buffer(6 + bytes.length);
        message.writeByte(Messages.f);
        message.writeZero(Messages.LENGTH_SIZE);// placeholder
        message.writeBytes(bytes);
        message.writeByte(Messages.STRING_TERMINATOR);

        Messages.writeLength(message);
        return message;
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

    /**
     * @see #handleCopyInResponse(ByteBuf)
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyData</a>
     */
    private void executeSendCopyInDataFromLocalPath(final Path path, final FluxSink<ByteBuf> sink) {

        ByteBuf message = null;
        try (FileChannel channel = FileChannel.open(path)) {
            long restSize = channel.size();
            final byte[] bufferArray = new byte[(int) Math.min(2048, restSize)];
            final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

            final int headerLength = 5, maxMessageLen = (headerLength + (bufferArray.length << 6));
            final ByteBufAllocator allocator = this.adjutant.allocator();
            message = allocator.buffer((int) Math.min(maxMessageLen, headerLength + restSize));

            message.writeByte(Messages.d);
            message.writeZero(Messages.LENGTH_SIZE);

            while (channel.read(buffer) > 0) {
                buffer.flip();
                message.writeBytes(bufferArray, buffer.position(), buffer.limit());
                buffer.clear();

                if (message.readableBytes() >= maxMessageLen) {
                    restSize -= (message.readableBytes() - headerLength);
                    Messages.writeLength(message);
                    sink.next(message);
                    message = allocator.buffer((int) Math.min(maxMessageLen, headerLength + restSize));
                    message.writeByte(Messages.d);
                    message.writeZero(Messages.LENGTH_SIZE);
                }
            }

            if (message.readableBytes() > headerLength) {
                Messages.writeLength(message);
                sink.next(message);
            } else {
                message.release();
            }
            sink.next(createCopyDoneMessage());
        } catch (Throwable e) {
            if (message != null) {
                message.release();
            }
            String msg = String.format("Copy-in read local file occur error,%s", e.getMessage());
            sink.next(createCopyFailMessage(msg));
        }
        sink.complete();

    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyDone</a>
     */
    private ByteBuf createCopyDoneMessage() {
        final ByteBuf message = this.adjutant.allocator().buffer(5);
        message.writeByte(Messages.c);
        message.writeInt(0);
        return message;
    }


    enum CopyMode {
        NONE,
        COPY_IN_MODE,
        COPY_OUT_MODE
    }


}
