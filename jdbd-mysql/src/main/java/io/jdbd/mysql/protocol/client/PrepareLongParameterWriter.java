package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html">Protocol::COM_STMT_SEND_LONG_DATA</a>
 */
final class PrepareLongParameterWriter implements PrepareExecuteCommandWriter.LongParameterWriter {

    private static final Logger LOG = LoggerFactory.getLogger(PrepareLongParameterWriter.class);

    private static final int LONG_DATA_PREFIX_SIZE = 7;

    private static final int BUFFER_SIZE = 2048;

    /**
     * chunk can't send multi packet,avoid long data error,handle error difficulty.
     */
    private static final int MAX_CHUNK_SIZE = Packets.MAX_PAYLOAD - LONG_DATA_PREFIX_SIZE - 1;

    private static final int MIN_CHUNK_SIZE = BUFFER_SIZE << 2;

    private final PrepareStmtTask stmtTask;

    private final int statementId;

    private final MySQLColumnMeta[] columnMetas;

    private final TaskAdjutant adjutant;

    private final Properties properties;

    private final int maxPayload;

    private final int maxPacket;


    PrepareLongParameterWriter(final PrepareStmtTask stmtTask) {
        this.stmtTask = stmtTask;
        this.statementId = stmtTask.getStatementId();
        this.adjutant = stmtTask.adjutant();
        this.properties = this.adjutant.host().getProperties();

        this.maxPayload = getMaxPayload();
        this.maxPacket = Packets.HEADER_SIZE + maxPayload;
        this.columnMetas = stmtTask.getParameterMetas();

    }


    @Override
    public Flux<ByteBuf> write(final int stmtIndex, List<? extends ParamValue> valueList) {
        return Flux.fromIterable(valueList)
                .flatMap(paramValue -> sendLongData(stmtIndex, paramValue));
    }


    /*################################## blow private method ##################################*/


    /**
     * @see #write(int, List)
     */
    private Publisher<ByteBuf> sendLongData(final int batchIndex, final ParamValue paramValue) {
        final Object value = paramValue.getNonNull();

        final Publisher<ByteBuf> flux;
        if (value instanceof Path) {
            flux = sendPathParameter(batchIndex, paramValue);
        } else if (value instanceof Publisher) {
            flux = new PacketSource(this, batchIndex, paramValue);
        } else {
            MySQLColumnMeta[] paramMetaArray = this.stmtTask.getParameterMetas();
            MySQLType mySQLType = paramMetaArray[paramValue.getIndex()].sqlType;
            flux = Flux.error(MySQLExceptions.createUnsupportedParamTypeError(batchIndex, mySQLType, paramValue));
        }
        return flux;
    }


    /**
     * @see #sendLongData(int, ParamValue)
     */
    private Publisher<ByteBuf> sendPathParameter(final int batchIndex, final ParamValue paramValue) {
        return Flux.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                sendPathParameterInEventLoop(batchIndex, paramValue, sink);
            } else {
                this.adjutant.execute(() -> sendPathParameterInEventLoop(batchIndex, paramValue, sink));
            }

        });
    }


    private void handleSendError(final Throwable error) {
        if (this.adjutant.inEventLoop()) {
            this.stmtTask.handleLongParamSendFailure(error);
        } else {
            this.adjutant.execute(() -> this.stmtTask.handleLongParamSendFailure(error));
        }
    }


    private void sendPublisherInEventLoop(final int batchIndex, final ParamValue paramValue
            , final FluxSink<ByteBuf> sink) {

    }

    private void sendPathParameterInEventLoop(final int batchIndex, final ParamValue paramValue
            , final FluxSink<ByteBuf> sink) {
        final MySQLType sqlType = this.columnMetas[paramValue.getIndex()].sqlType;
        switch (sqlType) {
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
                sendTextPathInEventLoop(batchIndex, paramValue, sink);
                break;
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case GEOMETRY:
                sendBinaryPathInEventLoop(batchIndex, paramValue, sink);
                break;
            default: {
                final Throwable e;
                e = JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, sqlType, paramValue);
                sink.complete(); // no send error.
                handleSendError(e);
            }
        }
    }


    /**
     * @see #sendPathParameterInEventLoop(int, ParamValue, FluxSink)
     */
    private void sendTextPathInEventLoop(final int batchIndex, final ParamValue paramValue
            , final FluxSink<ByteBuf> sink) {

        ByteBuf packet = null;
        try (FileChannel channel = FileChannel.open((Path) paramValue.getNonNull(), StandardOpenOption.READ)) {

            final Charset clientChart = this.adjutant.charsetClient();
            final boolean isUtf8 = StandardCharsets.UTF_8.equals(clientChart);
            final CharsetEncoder encoder;
            final CharsetDecoder decoder;

            if (isUtf8) {
                encoder = null;
                decoder = null;
            } else {
                encoder = clientChart.newEncoder();
                decoder = StandardCharsets.UTF_8.newDecoder();
            }
            final byte[] bufferArray = new byte[2048];

            final int paramIndex = paramValue.getIndex();
            packet = createLongDataPacket(paramIndex, channel.size());
            final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

            while (channel.read(buffer) > 0) {
                buffer.flip();
                if (isUtf8) {
                    packet = writeOneBuffer(packet, paramIndex, bufferArray, buffer.limit(), sink);
                } else {
                    final ByteBuffer decodedBuffer = encoder.encode(decoder.decode(buffer));
                    final byte[] tempBytes = new byte[decodedBuffer.remaining()];
                    decodedBuffer.get(tempBytes);
                    packet = writeOneBuffer(packet, paramIndex, tempBytes, tempBytes.length, sink);
                }
                buffer.clear();
            }
            handleLastPacket(packet, sink);
        } catch (Throwable e) {
            handleWriteError(packet, batchIndex, e, paramValue);
        } finally {
            // don't emit error
            sink.complete();
        }

    }

    /**
     * @see #sendPathParameterInEventLoop(int, ParamValue, FluxSink)
     */
    private void sendBinaryPathInEventLoop(final int batchIndex, final ParamValue paramValue
            , final FluxSink<ByteBuf> sink) {
        final int paramIndex = paramValue.getIndex();
        ByteBuf packet = null;
        try (FileChannel channel = FileChannel.open((Path) paramValue.getNonNull(), StandardOpenOption.READ)) {
            final byte[] bufferArray = new byte[2048];

            packet = createLongDataPacket(paramIndex, channel.size());
            final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

            while (channel.read(buffer) > 0) {
                buffer.flip();
                packet = writeOneBuffer(packet, paramIndex, bufferArray, buffer.limit(), sink);
                buffer.clear();
            }
            handleLastPacket(packet, sink);
        } catch (Throwable e) {
            handleWriteError(packet, batchIndex, e, paramValue);
        } finally {
            // don't emit error
            sink.complete();
        }
    }

    /**
     * @see #sendTextPathInEventLoop(int, ParamValue, FluxSink)
     * @see #sendBinaryPathInEventLoop(int, ParamValue, FluxSink)
     */
    private void handleLastPacket(ByteBuf packet, FluxSink<ByteBuf> sink) {
        if (packet.readableBytes() > Packets.HEADER_SIZE + LONG_DATA_PREFIX_SIZE) {
            Packets.writeHeader(packet, this.stmtTask.addAndGetSequenceId());
            sink.next(packet);
        } else {
            packet.release();
        }
    }

    /**
     * @see #sendTextPathInEventLoop(int, ParamValue, FluxSink)
     * @see #sendBinaryPathInEventLoop(int, ParamValue, FluxSink)
     */
    private void handleWriteError(@Nullable ByteBuf packet, int batchIndex, Throwable e, ParamValue paramValue) {
        if (packet != null && packet.refCnt() > 0) {
            packet.release();
        }
        BindValue bindValue;
        if (paramValue instanceof BindValue) {
            bindValue = (BindValue) paramValue;
        } else {
            int paramIndex = paramValue.getIndex();
            bindValue = BindValue.wrap(paramIndex, this.columnMetas[paramIndex].sqlType, paramValue.get());
        }
        Throwable error = MySQLExceptions.createLongDataReadException(batchIndex, bindValue, e);
        handleSendError(error);
    }

    private ByteBuf writeOneBuffer(ByteBuf packet, final int paramIndex, final byte[] buffer, final int length
            , final FluxSink<ByteBuf> sink) {
        if (length < 0 || length > buffer.length) {
            throw new IllegalArgumentException("length error");
        }
        final int maxWritableBytes = packet.maxWritableBytes();
        if (maxWritableBytes > length) {
            packet.writeBytes(buffer, 0, length);
        } else {
            packet.writeBytes(buffer, 0, maxWritableBytes);
            Packets.writeHeader(packet, this.stmtTask.addAndGetSequenceId());
            sink.next(packet);

            final int resetLength = length - maxWritableBytes;
            packet = createLongDataPacket(paramIndex, resetLength);
            packet.writeBytes(buffer, maxWritableBytes, resetLength);

        }
        return packet;
    }


    private ByteBuf createLongDataPacket(final int parameterIndex, final long chunkSize) {
        final int capacity;
        if (chunkSize < 1024) {
            capacity = BUFFER_SIZE;
        } else {
            capacity = (int) Math.min(this.maxPayload, chunkSize);
        }
        final ByteBuf packet = this.adjutant.allocator().buffer(capacity, maxPacket);
        packet.writeZero(Packets.HEADER_SIZE); // placeholder of header

        packet.writeByte(Packets.COM_STMT_SEND_LONG_DATA); //status
        Packets.writeInt4(packet, this.statementId); //statement_id
        Packets.writeInt2(packet, parameterIndex);//param_id
        return packet;

    }

    private int getMaxPayload() {
        int chunkSize = this.properties.getOrDefault(MyKey.blobSendChunkSize, Integer.class);
        final int maxChunkSize = Math.min(this.adjutant.mysqlUrl().getMaxAllowedPayload(), MAX_CHUNK_SIZE);
        if (chunkSize < MIN_CHUNK_SIZE) {
            chunkSize = MIN_CHUNK_SIZE;
        } else if (chunkSize > maxChunkSize) {
            chunkSize = maxChunkSize;
        }
        return chunkSize;
    }


    private Charset obtainCharset(final int paramIndex) {
        final Charset charset;
        if (this.stmtTask.getParameterMetas()[paramIndex].sqlType.isLongString()) {
            charset = obtainClobCharset();
        } else {
            charset = this.adjutant.charsetClient();
        }
        return charset;
    }

    /**
     * @see #writeCharArray(ByteBuf, FluxSink, int, char[], int)
     */
    private Charset obtainClobCharset() {
        Charset charset = this.properties.get(MyKey.clobCharacterEncoding, Charset.class);
        if (charset == null) {
            charset = this.adjutant.charsetClient();
        }
        return charset;
    }


    /*################################## blow private instance inner class ##################################*/

    private static final class PacketSource implements Publisher<ByteBuf> {

        private final PrepareLongParameterWriter parameterWriter;

        private final int batchIndex;

        private final ParamValue paramValue;


        private PacketSource(PrepareLongParameterWriter parameterWriter, int batchIndex, ParamValue paramValue) {
            this.parameterWriter = parameterWriter;
            this.batchIndex = batchIndex;
            this.paramValue = paramValue;
        }

        @Override
        public void subscribe(Subscriber<? super ByteBuf> s) {
            try {
                final PacketSubscription subscription;
                subscription = new PacketSubscription(this.parameterWriter, this.batchIndex, this.paramValue, s);
                ((Publisher<?>) this.paramValue.getNonNull()).subscribe(subscription);
                s.onSubscribe(subscription);
            } catch (Throwable e) {
                s.onError(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        }

    }


    private static final class PacketSubscription implements Subscription, Subscriber<Object> {

        private final PrepareLongParameterWriter parameterWriter;

        private final int batchIndex;

        private final ParamValue paramValue;

        private final Subscriber<? super ByteBuf> subscriber;

        private ByteBuf packet;

        private Subscription upstream;

        private PacketSubscription(PrepareLongParameterWriter parameterWriter, int batchIndex
                , ParamValue paramValue, Subscriber<? super ByteBuf> subscriber) {
            this.parameterWriter = parameterWriter;
            this.batchIndex = batchIndex;
            this.paramValue = paramValue;
            this.subscriber = subscriber;

            subscriber.onSubscribe(this);
        }

        @Override
        public void request(long n) {
            final Subscription s = this.upstream;
            if (s != null) {
                s.request(n);
            }

        }

        @Override
        public void cancel() {
            final Subscription s = this.upstream;
            if (s != null) {
                s.cancel();
            }
            if (this.parameterWriter.adjutant.inEventLoop()) {
                releasePacketInEventLoop();
            } else {
                this.parameterWriter.adjutant.execute(this::releasePacketInEventLoop);
            }

        }

        @Override
        public void onSubscribe(Subscription s) {
            this.upstream = s;
        }

        @Override
        public void onNext(final Object item) {
            if (!(item instanceof byte[])) {
                return;
            }


        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onComplete() {

        }

        private void releasePacketInEventLoop() {
            final ByteBuf packet = this.packet;
            if (packet != null && packet.refCnt() > 0) {
                packet.release();
                this.packet = null;
            }
        }

    }


}
