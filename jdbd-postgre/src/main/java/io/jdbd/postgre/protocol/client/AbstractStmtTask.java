package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.util.PgArrays;
import io.jdbd.stmt.LocalFileException;
import io.jdbd.vendor.result.FluxResultSink;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
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

    private int resultIndex = 0;

    private boolean downstreamCanceled;

    AbstractStmtTask(TaskAdjutant adjutant, FluxResultSink sink) {
        super(adjutant, sink::error);
        this.sink = sink;
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


    final void handleCopyInResponse(ByteBuf cumulateBuffer, List<String> sqlList) {
        final int msgStartIndex = cumulateBuffer.readerIndex();
        if (cumulateBuffer.readByte() != Messages.G) {
            throw new IllegalArgumentException("Non Copy-In message.");
        }
        cumulateBuffer.readerIndex(msgStartIndex + 1 + cumulateBuffer.readInt()); // skip CopyInResponse message,this message design too stupid ,it should return filename of STDIN,but not.
        final PgParser parser = this.adjutant.sqlParser();
        Path path = null;
        Throwable parseError = null;
        try {
            path = parser.parseCopyInPath(sqlList.get(this.resultIndex));
        } catch (Throwable e) {
            parseError = e;
        }

        if (parseError != null) {
            String msg = String.format("Parse copy in sql error,%s", parseError.getMessage());
            this.packetPublisher = createCopyFailMessage(msg);
        } else if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            String msg = String.format("filename[%s] not exists.", path.toAbsolutePath());
            this.packetPublisher = createCopyFailMessage(msg);
        } else if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            String msg = String.format("filename[%s] is directory.", path.toAbsolutePath());
            this.packetPublisher = createCopyFailMessage(msg);
        } else if (!Files.isReadable(path)) {
            String msg = String.format("filename[%s] isn't readable.", path.toAbsolutePath());
            this.packetPublisher = createCopyFailMessage(msg);
        } else {
            this.packetPublisher = sendCopyDataForCopyIn(path);
        }
    }

    private Publisher<ByteBuf> sendCopyDataForCopyIn(final Path path) {
        return Flux.create(sink -> {
            try {
                executeSendCopyDataForCopyIn(path, sink);
                sink.complete();
            } catch (Throwable e) {
                String msg = String.format("Local file[%s] copy in occur error.", path);
                sink.error(new LocalFileException(path, msg, e));
            }
        });
    }

    /**
     * @see #sendCopyDataForCopyIn(Path)
     */
    private void executeSendCopyDataForCopyIn(final Path path, FluxSink<ByteBuf> sink) throws Exception {
        try (FileChannel channel = FileChannel.open(path)) {
            final long fileSize = channel.size();
            final byte[] bufferArray = new byte[(int) Math.min(2048, fileSize)];
            final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);
            long restSize = fileSize;

            final ByteBufAllocator allocator = this.adjutant.allocator();
            ByteBuf message = allocator.buffer();

            message.writeByte(Messages.d);
            message.writeZero(Messages.LENGTH_SIZE);

            while (channel.read(buffer) > 0) {
                buffer.flip();
                message.writeBytes(bufferArray, buffer.position(), buffer.limit());
            }


        }


    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyFail</a>
     */
    private Publisher<ByteBuf> createCopyFailMessage(String errorInfo) {
        final byte[] bytes = errorInfo.getBytes(this.adjutant.clientCharset());
        final ByteBuf message = this.adjutant.allocator().buffer(6 + bytes.length);
        message.writeByte('f');
        message.writeZero(Messages.LENGTH_SIZE);// placeholder
        message.writeBytes(bytes);
        message.writeByte(Messages.STRING_TERMINATOR);

        Messages.writeLength(message);
        return Mono.just(message);
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
                if (UPDATE_COMMANDS.contains(command.toUpperCase())) {
                    params.affectedRows = Long.parseLong(tokenizer.nextToken());
                }
            }
            break;
            case 3: {
                command = tokenizer.nextToken();
                params.insertId = Long.parseLong(tokenizer.nextToken());
                if (UPDATE_COMMANDS.contains(command.toUpperCase())) {
                    params.affectedRows = Long.parseLong(tokenizer.nextToken());
                }
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
