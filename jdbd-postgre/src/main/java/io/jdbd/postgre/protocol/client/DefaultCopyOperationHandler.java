package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.syntax.CopyIn;
import io.jdbd.postgre.syntax.CopyOperation;
import io.jdbd.postgre.syntax.CopyOut;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.statement.BindSingleStatement;
import io.jdbd.statement.StaticStatement;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import io.jdbd.vendor.stmt.Stmt;
import io.jdbd.vendor.util.FunctionWithError;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.qinarmy.util.UnexpectedEnumException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;

final class DefaultCopyOperationHandler implements CopyOperationHandler {

    static DefaultCopyOperationHandler create(PgCommandTask task, Consumer<Publisher<ByteBuf>> messageSender) {
        return new DefaultCopyOperationHandler(task, messageSender);
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultCopyOperationHandler.class);

    private final PgCommandTask task;

    private final Consumer<Publisher<ByteBuf>> messageSender;

    private final TaskAdjutant adjutant;

    private CopyOperation cacheCopyOperation;

    private CopyOutHandler copyOutHandler;

    private List<String> singleSqlList;


    private DefaultCopyOperationHandler(PgCommandTask task, Consumer<Publisher<ByteBuf>> messageSender) {
        this.task = task;
        this.messageSender = messageSender;
        this.adjutant = task.adjutant();
    }

    @Override
    public final void handleCopyInResponse(final ByteBuf cumulateBuffer) {
        final int msgStartIndex = cumulateBuffer.readerIndex();
        if (cumulateBuffer.readByte() != Messages.G) {
            throw new IllegalArgumentException("Non Copy-In message.");
        }
        cumulateBuffer.readerIndex(msgStartIndex + 1 + cumulateBuffer.readInt()); // skip CopyInResponse message,this message design too stupid ,it should return filename of STDIN,but not.

        final int resultIndex = this.task.getResultIndex();
        Publisher<ByteBuf> publisher;
        try {
            final CopyIn copyIn = parseCopyOperation(resultIndex, this.adjutant.sqlParser()::parseCopyIn);
            switch (copyIn.getMode()) {
                case FILE:
                    publisher = sendCopyInDataFromLocalPath(obtainPathFromCopyOperation(resultIndex, copyIn));
                    break;
                case PROGRAM:
                case STDIN:
                    String msg = String.format("COPY FROM %s not supported by jdbd-postgre .", copyIn.getMode());
                    publisher = Mono.just(createCopyFailMessage(msg));
                    break;
                default:
                    throw PgExceptions.createUnexpectedEnumException(copyIn.getMode());
            }
        } catch (SQLException e) {
            String msg = String.format("Parse copy in sql error,%s", e.getMessage());
            publisher = Mono.just(createCopyFailMessage(msg));
        } catch (Throwable e) {
            String msg = String.format("Handle statement[index:%s] copy-in failure,message:%s"
                    , resultIndex, e.getMessage());
            publisher = Mono.just(createCopyFailMessage(msg));
        }
        this.messageSender.accept(Objects.requireNonNull(publisher));
    }

    @Override
    public final boolean handleCopyOutResponse(final ByteBuf cumulateBuffer) {
        final int msgStartIndex = cumulateBuffer.readerIndex();
        if (cumulateBuffer.readByte() != Messages.H) {
            throw new IllegalArgumentException("Non Copy-out message.");
        }
        final int nextMsgIndex = msgStartIndex + 1 + cumulateBuffer.readInt();
        cumulateBuffer.readerIndex(nextMsgIndex);

        if (this.copyOutHandler != null) {
            throw new IllegalStateException("this.copyOutHandler non-null,cannot handle CopyOutResponse message.");
        }

        if (this.task.hasError()) {
            this.copyOutHandler = new SubscriberCopyOutHandler(this.task); // skip all copy data.
            return handleCopyOutData(cumulateBuffer);
        }

        final int resultIndex = this.task.getResultIndex();
        boolean handleEnd;
        try {
            final CopyOut copyOut = parseCopyOperation(resultIndex, this.adjutant.sqlParser()::parseCopyOut);
            switch (copyOut.getMode()) {
                case FILE: {
                    final Path path = obtainPathFromCopyOperation(resultIndex, copyOut);
                    this.copyOutHandler = new LocalFileCopyOutHandler(this.task, path);
                    handleEnd = handleCopyOutData(cumulateBuffer);
                }
                break;
                case PROGRAM:
                case STDOUT: {
                    final Subscriber<byte[]> subscriber = obtainCopyOutSubscriberFromCopyOut(resultIndex, copyOut);
                    this.copyOutHandler = new SubscriberCopyOutHandler(this.task, subscriber);
                    handleEnd = handleCopyOutData(cumulateBuffer);
                }
                break;
                default:
                    throw PgExceptions.createUnexpectedEnumException(copyOut.getMode());
            }
        } catch (Throwable e) {
            final Throwable error;
            if (e instanceof SQLException) {
                String msg = String.format("Parse copy out sql error,%s", e.getMessage());
                error = new JdbdSQLException(msg, (SQLException) e);
            } else {
                String msg = String.format("Handle statement[index:%s] copy-out failure,message:%s"
                        , resultIndex, e.getMessage());
                error = new JdbdException(msg, e);
            }
            this.task.addErrorToTask(error);
            this.copyOutHandler = new SubscriberCopyOutHandler(this.task); // skip all copy data.
            handleEnd = handleCopyOutData(cumulateBuffer);
        }
        return handleEnd;
    }

    @Override
    public final boolean handleCopyOutData(final ByteBuf cumulateBuffer) {
        final CopyOutHandler handler = this.copyOutHandler;
        if (handler == null) {
            throw new IllegalStateException("No copy-out handler");
        }
        final boolean handleEnd;

        handleEnd = handler.handle(cumulateBuffer);
        if (handleEnd) {
            this.copyOutHandler = null;
        }
        return handleEnd;
    }

    @Override
    public final String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append(getClass().getSimpleName())
                .append("[cacheCopyOperation:")
                .append(this.cacheCopyOperation)
                .append(",copyOutHandler:")
                .append(this.copyOutHandler)
                .append(",singleSqlList size:");

        final List<String> singleSqlList = this.singleSqlList;
        if (singleSqlList == null) {
            builder.append("null");
        } else {
            builder.append(singleSqlList.size());
        }
        return builder
                .append("]")
                .toString();
    }


    /*################################## blow private method ##################################*/


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
        message.writeZero(Messages.LENGTH_BYTES);// placeholder
        message.writeBytes(bytes);
        message.writeByte(Messages.STRING_TERMINATOR);

        Messages.writeLength(message);
        return message;
    }

    /**
     * @see #handleCopyInResponse(ByteBuf)
     * @see #handleCopyOutResponse(ByteBuf)
     */
    private Path obtainPathFromCopyOperation(final int resultIndex, final CopyOperation copyOperation) {
        final int bindIndex = copyOperation.getBindIndex();
        final Stmt stmt = this.task.stmt;
        final Path path;
        if (bindIndex < 0) {
            path = copyOperation.getPath();
        } else if (stmt instanceof BindStmt) {
            path = obtainPathFromParamGroup(((BindStmt) stmt).getBindGroup(), resultIndex, bindIndex);
        } else if (stmt instanceof BindBatchStmt) {
            final List<List<BindValue>> groupList = ((BindBatchStmt) stmt).getGroupList();
            if (resultIndex >= groupList.size()) {
                // here  bug
                throw new IllegalStateException(String.format("IllegalState can't found COPY command,%s", this));
            }
            path = obtainPathFromParamGroup(groupList.get(resultIndex), resultIndex, bindIndex);
        } else if (stmt instanceof BindMultiStmt) {
            final List<BindStmt> stmtGroup = ((BindMultiStmt) stmt).getStmtList();
            if (resultIndex >= stmtGroup.size()) {
                // here  bug
                throw new IllegalStateException(String.format("IllegalState can't found COPY command,%s", this));
            }
            final List<BindValue> valueList = stmtGroup.get(resultIndex).getBindGroup();
            path = obtainPathFromParamGroup(valueList, resultIndex, bindIndex);
        } else {
            throw new IllegalStateException(String.format("Can't obtain path from Stmt[%s].", stmt));
        }
        return path;
    }


    /**
     * @see #obtainPathFromCopyOperation(int, CopyOperation)
     */
    private Path obtainPathFromParamGroup(final List<BindValue> valueList, final int resultIndex, final int bindIndex) {
        if (bindIndex >= valueList.size()) {
            //here  bug
            String m;
            m = String.format("IllegalState can't obtain 'filename' for COPY operation,Stmt[index:%s],bind[index:%s]"
                    , resultIndex, bindIndex);
            throw new IllegalStateException(m);
        }
        final Object value = valueList.get(bindIndex).get();
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
            message.writeZero(Messages.LENGTH_BYTES);

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
                    message.writeZero(Messages.LENGTH_BYTES);
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
     * @see #handleCopyOutResponse(ByteBuf)
     */
    private Subscriber<byte[]> obtainCopyOutSubscriberFromCopyOut(final int resultIndex, final CopyOut copyOut)
            throws SQLException {
        if (copyOut.getMode() == CopyOut.Mode.FILE) {
            throw new IllegalArgumentException("CopyOut is Mode.FILE");
        }

        final Stmt stmt = this.task.stmt;

        final PgParser parser = this.adjutant.sqlParser();
        final Function<Object, Subscriber<byte[]>> function;

        if (stmt instanceof StaticStmt) {
            if (resultIndex != 0 || !parser.isSingleStmt(((StaticStmt) stmt).getSql())) {
                throw new SQLException(String.format("COPY-OUT only is supported with single statement in %s"
                        , StaticStatement.class.getName()));
            }
            function = stmt.getExportFunction();
        } else if (stmt instanceof StaticBatchStmt) {
            final List<String> sqlGroup = ((StaticBatchStmt) stmt).getSqlGroup();
            if (resultIndex != 0 || sqlGroup.size() != 1) {
                throw new SQLException(String.format("COPY-OUT only is supported with single statement in %s"
                        , StaticStatement.class.getName()));
            }
            function = stmt.getExportFunction();
        } else if (stmt instanceof BindStmt) {
            if (resultIndex != 0) {
                // here 1. bug ; 2. postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                throw new SQLException(String.format("COPY-OUT only is supported with single result in %s"
                        , BindSingleStatement.class.getName()));
            }
            function = stmt.getExportFunction();
        } else if (stmt instanceof BindBatchStmt) {
            final List<List<BindValue>> groupList = ((BindBatchStmt) stmt).getGroupList();
            if (resultIndex != 0 || groupList.size() != 1) {
                throw new SQLException(String.format("COPY-OUT only is supported with single bind in %s"
                        , BindSingleStatement.class.getName()));
            }
            function = stmt.getExportFunction();
        } else if (stmt instanceof BindMultiStmt) {
            final List<BindStmt> stmtGroup = ((BindMultiStmt) stmt).getStmtList();
            if (resultIndex >= stmtGroup.size()) {
                // here 1. bug ; 2. postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                throw new SQLException("Not found Subscriber for COPY-OUT.");
            }
            function = stmtGroup.get(resultIndex).getExportFunction();
        } else {
            String m = String.format("Unknown %s type[%s]", Stmt.class.getName(), stmt.getClass().getName());
            throw new IllegalStateException(m);
        }
        if (function == null) {
            throw new SQLException(String.format("Not specify export function for statement[index:%s]", resultIndex));
        }

        try {
            final Subscriber<byte[]> subscriber;
            switch (copyOut.getMode()) {
                case STDOUT:
                    subscriber = function.apply(this.adjutant.clientCharset());
                    break;
                case PROGRAM:
                    subscriber = function.apply(copyOut.getCommand());
                    break;
                default:
                    throw PgExceptions.createUnexpectedEnumException(copyOut.getMode());
            }
            return subscriber;
        } catch (UnexpectedEnumException e) {
            throw e;
        } catch (Throwable e) {
            throw new ExportSubscriberFunctionException(e.getMessage(), e);
        }

    }


    /**
     * @see #handleCopyInResponse(ByteBuf)
     */
    @SuppressWarnings("unchecked")
    private <T extends CopyOperation> T parseCopyOperation(final int resultIndex
            , final FunctionWithError<String, T> function) throws Exception {

        final Stmt stmt = this.task.stmt;
        final T copyOperation;
        if (stmt instanceof StaticStmt) {
            List<String> singleSqlList = this.singleSqlList;
            if (singleSqlList == null) {
                singleSqlList = this.adjutant.sqlParser().separateMultiStmt(((StaticStmt) stmt).getSql());
                this.singleSqlList = singleSqlList;
            }
            if (resultIndex >= singleSqlList.size()) {
                // here 1. bug ; 2. postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                throw new IllegalStateException(String.format("IllegalState can't found COPY command,%s", this));
            }
            copyOperation = function.apply(singleSqlList.get(resultIndex));
        } else if (stmt instanceof StaticBatchStmt) {
            final List<String> sqlGroup = ((StaticBatchStmt) stmt).getSqlGroup();
            if (resultIndex >= sqlGroup.size()) {
                // here 1. bug ; 2. postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                throw new IllegalStateException(String.format("IllegalState can't found COPY command,%s", this));
            }
            copyOperation = function.apply(sqlGroup.get(resultIndex));
        } else if (stmt instanceof BindBatchStmt) {
            CopyOperation cacheCopy = this.cacheCopyOperation;
            if (cacheCopy == null) {
                cacheCopy = function.apply(((BindBatchStmt) stmt).getSql());
                this.cacheCopyOperation = cacheCopy;
            }
            final List<List<BindValue>> groupList = ((BindBatchStmt) stmt).getGroupList();
            if (resultIndex >= groupList.size()) {
                // here 1. bug ; 2. postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                throw new SQLException(String.format("IllegalState can't found COPY command,%s", this));
            }
            copyOperation = (T) cacheCopy;
        } else if (stmt instanceof BindStmt) {
            if (resultIndex != 0) {
                // here  postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                String m;
                m = String.format("Postgre response %s CommendComplete message,but expect one.", resultIndex + 1);
                throw new JdbdException(m);
            }
            copyOperation = function.apply(((BindStmt) stmt).getSql());
        } else if (stmt instanceof BindMultiStmt) {
            final List<BindStmt> stmtGroup = ((BindMultiStmt) stmt).getStmtList();
            if (resultIndex >= stmtGroup.size()) {
                // here 1. bug ; 2. postgre CALL command add new feature that CALL command can return multi CommendComplete message.
                throw new IllegalStateException(String.format("IllegalState can't found COPY command,%s", this));
            }
            copyOperation = function.apply(stmtGroup.get(resultIndex).getSql());
        } else {
            String m = String.format("Unknown %s type[%s]", Stmt.class.getName(), stmt.getClass().getName());
            throw new IllegalStateException(m);
        }
        return copyOperation;
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


    private static JdbdSQLException readCopyFailMessage(final ByteBuf cumulateBuffer, PgCommandTask task) {
        if (cumulateBuffer.readByte() != Messages.f) {
            throw new IllegalArgumentException("Non-CopyFail message");
        }
        cumulateBuffer.readInt(); // skip length
        String msg = Messages.readString(cumulateBuffer, task.adjutant().clientCharset());
        SQLException e;
        e = new SQLException(msg);
        return new JdbdSQLException(e);
    }


    private interface CopyOutHandler {

        /**
         * <p>
         * This method cannot throw any Throwable
         * </p>
         *
         * @return true : copy out end.
         */
        boolean handle(ByteBuf cumulateBuffer);

    }

    private static final class LocalFileCopyOutHandler implements CopyOutHandler {

        private final PgCommandTask task;

        private final FileChannel channel;

        final ByteBuffer buffer;

        private LocalFileCopyOutHandler(PgCommandTask task, Path path) {
            this.task = task;
            FileChannel channel;
            try {
                final Path dir = path.getParent();
                if (!Files.exists(dir)) {
                    Files.createDirectories(dir);
                }
                channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            } catch (Throwable e) {
                channel = null;
                task.addErrorToTask(e);
            }
            this.channel = channel;

            if (channel == null) {
                this.buffer = null;
            } else {
                this.buffer = ByteBuffer.wrap(new byte[2048]);
            }

        }

        @Override
        public final boolean handle(final ByteBuf cumulateBuffer) {
            boolean copyEnd = false;
            int nextMsgIndex = -1;
            try {
                final FileChannel channel = this.channel;
                final ByteBuffer buffer = this.buffer;
                loop:
                while (Messages.hasOneMessage(cumulateBuffer)) {
                    final int msgIndex = cumulateBuffer.readerIndex();
                    nextMsgIndex = msgIndex + 1 + cumulateBuffer.getInt(msgIndex + 1);

                    switch (cumulateBuffer.getByte(msgIndex)) {
                        case Messages.d: {// CopyData message
                            if (channel != null) {  // if channel is null ,occur error,eg: can't create file.
                                writeOneCopyDataMessageToLocalPath(channel, cumulateBuffer, buffer);
                            }
                        }
                        break;
                        case Messages.c: {// CopyDone message
                            copyEnd = true; // must be end copy firstly,because possibly throw exception
                            cumulateBuffer.readerIndex(nextMsgIndex); // skip CopyDone message
                            if (channel != null) {//if channel is null ,occur error,eg: can't create file.
                                channel.close();
                            }
                        }
                        break loop;
                        case Messages.f: {// CopyFail message
                            copyEnd = true; // must be end copy firstly,because possibly throw exception
                            this.task.addErrorToTask(readCopyFailMessage(cumulateBuffer, this.task));
                            if (channel != null) {//if channel is null ,occur error,eg: can't create file.
                                channel.close();
                            }
                            cumulateBuffer.readerIndex(nextMsgIndex); // avoid tail filler
                        }
                        break loop;
                        default:
                            String m = String.format("Message[%s] Not Copy-out relation message."
                                    , (char) cumulateBuffer.getByte(msgIndex));
                            throw new UnExpectedMessageException(m);
                    }
                    cumulateBuffer.readerIndex(nextMsgIndex); // finally avoid tail filler
                }
            } catch (Throwable e) {
                if (!(e instanceof UnExpectedMessageException) && nextMsgIndex > 0) {
                    cumulateBuffer.readerIndex(nextMsgIndex); //avoid exception but not read rest of message.
                }
                this.task.addErrorToTask(e);
                this.task.log.debug("Copy-Out occur error.task:{}", this.task, e);
            }
            return copyEnd;
        }

        /***
         * @param buffer must be created by ByteBuffer.wrap(bufferArray)
         */
        private static void writeOneCopyDataMessageToLocalPath(FileChannel channel, ByteBuf cumulateBuffer
                , ByteBuffer buffer)
                throws IOException {
            if (cumulateBuffer.readByte() != Messages.d) {// CopyData message
                throw new IllegalArgumentException("Not CopyData message.");
            }
            int restLength = cumulateBuffer.readInt() - Messages.LENGTH_BYTES;
            final byte[] bufferArray = buffer.array(); // buffer must be created by ByteBuffer.wrap(bufferArray);
            while (restLength > 0) {
                final int readLength = Math.min(restLength, bufferArray.length);
                cumulateBuffer.readBytes(bufferArray, 0, readLength);
                restLength -= readLength;

                buffer.clear();
                buffer.limit(readLength);

                channel.write(buffer);
            }

        }


    }// class LocalFileCopyOutHandler

    private static final class SubscriberCopyOutHandler implements CopyOutHandler, Subscription {

        private static final AtomicIntegerFieldUpdater<SubscriberCopyOutHandler> CANCEL
                = AtomicIntegerFieldUpdater.newUpdater(SubscriberCopyOutHandler.class, "cancel");

        private final PgCommandTask task;

        private final Subscriber<byte[]> subscriber;

        /**
         * <ul>
         *     <li>1:cancel </li>
         * </ul>
         */
        private volatile int cancel = 0;

        /**
         * skip all CopyOut data
         */
        private SubscriberCopyOutHandler(PgCommandTask task) {
            this.task = task;
            this.subscriber = null;
        }

        private SubscriberCopyOutHandler(PgCommandTask task, final Subscriber<byte[]> subscriber) {
            this.task = task;
            Subscriber<byte[]> actualSubscriber;
            try {
                subscriber.onSubscribe(this);
                actualSubscriber = subscriber;
            } catch (Throwable e) {
                task.addErrorToTask(e);
                actualSubscriber = null;
            }
            this.subscriber = actualSubscriber;

        }

        @Override
        public final boolean handle(final ByteBuf cumulateBuffer) {
            boolean copyEnd = false;
            int nextMsgIndex = -1;

            try {
                final Subscriber<byte[]> subscriber = this.subscriber;

                loop:
                while (Messages.hasOneMessage(cumulateBuffer)) {
                    final int msgIndex = cumulateBuffer.readerIndex();
                    nextMsgIndex = msgIndex + 1 + cumulateBuffer.getInt(msgIndex + 1);

                    switch (cumulateBuffer.getByte(msgIndex)) {
                        case Messages.d: {// CopyData message
                            if (subscriber != null && this.cancel != 1) {
                                cumulateBuffer.readByte();// skip msg type byte
                                final byte[] rowBytes = new byte[cumulateBuffer.readInt() - Messages.LENGTH_BYTES];
                                cumulateBuffer.readBytes(rowBytes);
                                subscriber.onNext(rowBytes);
                            }
                        }
                        break;
                        case Messages.c: {// CopyDone message
                            copyEnd = true; // must be end copy firstly,because possibly throw exception
                            cumulateBuffer.readerIndex(nextMsgIndex); // skip CopyDone message
                            if (subscriber != null) {
                                subscriber.onComplete();
                            }
                        }
                        break loop;
                        case Messages.f: {// CopyFail message
                            copyEnd = true; // must be end copy firstly,because possibly throw exception
                            if (subscriber != null) {
                                final JdbdSQLException error = readCopyFailMessage(cumulateBuffer, this.task);
                                this.task.addErrorToTask(error); // firstly
                                subscriber.onError(error); // secondly
                            }
                            cumulateBuffer.readerIndex(nextMsgIndex); // skip CopyFail message
                        }
                        break loop;
                        default:
                            String m = String.format("Message[%s] Not Copy-out relation message."
                                    , (char) cumulateBuffer.getByte(msgIndex));
                            throw new UnExpectedMessageException(m);
                    }
                    cumulateBuffer.readerIndex(nextMsgIndex); // finally avoid tail filler
                }
            } catch (Throwable e) {
                if (!(e instanceof UnExpectedMessageException) && nextMsgIndex > 0) {
                    cumulateBuffer.readerIndex(nextMsgIndex); //avoid exception but not read rest of message.
                }
                this.task.addErrorToTask(e);
                this.task.log.debug("Copy-Out occur error.task:{}", this.task, e);
            }
            return copyEnd;
        }

        @Override
        public final void request(long n) {
            // no-ope,ignore this method,@see io.jdbd.stmt.Statement
        }

        @Override
        public final void cancel() {
            CANCEL.compareAndSet(this, 0, 1);
        }


    }// class SubscriberCopyOutHandler


}
