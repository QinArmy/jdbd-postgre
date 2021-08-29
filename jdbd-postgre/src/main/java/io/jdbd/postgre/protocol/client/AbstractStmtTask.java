package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.postgre.stmt.MultiBindStmt;
import io.jdbd.postgre.syntax.CopyIn;
import io.jdbd.postgre.util.PgArrays;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.vendor.result.FluxResultSink;
import io.jdbd.vendor.stmt.IoAbleStmt;
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
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.Function;
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

    final IoAbleStmt ioAbleStmt;

    private int resultIndex = 0;

    private boolean downstreamCanceled;

    AbstractStmtTask(TaskAdjutant adjutant, FluxResultSink sink, @Nullable IoAbleStmt ioAbleStmt) {
        super(adjutant, sink::error);
        this.sink = sink;
        this.ioAbleStmt = ioAbleStmt;
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
            final CopyIn copyIn;
            copyIn = this.adjutant.sqlParser().parseCopyIn(obtainCopyOperationStmt(resultIndex), resultIndex);

            switch (copyIn.getMode()) {
                case FILE:
                    publisher = sendCopyInDataFromLocalPath(copyIn.getPath());
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
            String msg = String.format("Handle statement[index:%s] copy in failure,message:%s"
                    , resultIndex, e.getMessage());
            publisher = Mono.just(createCopyFailMessage(msg));
        }
        this.packetPublisher = Objects.requireNonNull(publisher);
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
     * @see #handleCopyInResponse(ByteBuf)
     */
    @Deprecated
    private Publisher<ByteBuf> sendCopyInDataFromProgramCommand(CopyIn copyIn) {
        final Function<String, Publisher<byte[]>> function = copyIn.getFunction();
        final Publisher<byte[]> dataPublisher = function.apply(copyIn.getCommand());

        final Publisher<ByteBuf> publisher;
        if (dataPublisher == null) {
            String m = String.format("Statement[index:%s] PROGRAM 'command; function return Publisher is null."
                    , this.resultIndex);
            publisher = Mono.just(createCopyFailMessage(m));
        } else {
            publisher = Flux.from(dataPublisher)
                    .map(this::mapToCopyDataMessage)
                    .concatWith(Mono.create(sink -> sink.success(createCopyDoneMessage())))
                    .onErrorResume(e -> Mono.just(createCopyFailMessage(e.getMessage())));
        }
        return publisher;
    }

    /**
     * @see #handleCopyInResponse(ByteBuf)
     */
    @Deprecated
    private Publisher<ByteBuf> sendCopyInDataFromStdin(CopyIn copyIn) {
        final Function<String, Publisher<byte[]>> function = copyIn.getFunction();
        final Publisher<byte[]> dataPublisher = function.apply(null);

        final Publisher<ByteBuf> publisher;
        if (dataPublisher == null) {
            String m = String.format("Statement[index:%s] STDIN function return Publisher is null."
                    , this.resultIndex);
            publisher = Mono.just(createCopyFailMessage(m));
        } else {
            publisher = Flux.from(dataPublisher)
                    .map(this::mapToCopyDataMessage)
                    .concatWith(Mono.create(sink -> sink.success(createCopyDoneMessage())))
                    .onErrorResume(e -> Mono.just(createCopyFailMessage(e.getMessage())));
        }
        return publisher;
    }


    /**
     * @see #handleCopyInResponse(ByteBuf)
     */
    private Stmt obtainCopyOperationStmt(final int resultIndex) throws IllegalStateException {
        final IoAbleStmt ioAbleStmt = this.ioAbleStmt;
        if (ioAbleStmt == null) {
            String m = "Your Subscriber method not support COPY IN operation,please use other method.";
            throw new IllegalStateException(m);
        }
        final Stmt stmt;
        if (ioAbleStmt instanceof MultiBindStmt) {
            final List<BindableStmt> stmtList = ((MultiBindStmt) ioAbleStmt).getStmtGroup();
            if (resultIndex < stmtList.size()) {
                stmt = stmtList.get(resultIndex);
            } else {
                // here ,bug
                String m = String.format("Reject copy ,Result index[%s] error,not in [0,%s)"
                        , resultIndex, stmtList.size());
                throw new IllegalStateException(m);
            }
        } else if (ioAbleStmt instanceof Stmt) {
            if (resultIndex != 0) {
                String m = String.format("Reject copy ,Result index[%s] error,not in [0,1)", resultIndex);
                throw new IllegalStateException(m);
            }
            stmt = (Stmt) ioAbleStmt;
        } else {
            throw new IllegalStateException(String.format("Unknown %s type[%s]", IoAbleStmt.class.getName()
                    , ioAbleStmt.getClass().getName()));
        }
        return stmt;
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
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyData</a>
     */
    private ByteBuf mapToCopyDataMessage(final byte[] dataBytes) {
        final ByteBuf message = this.adjutant.allocator().buffer(5 + dataBytes.length);
        message.writeByte(Messages.d);
        message.writeZero(Messages.LENGTH_SIZE);//placeholder of length
        message.writeBytes(dataBytes);

        Messages.writeLength(message);
        return message;
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


}
