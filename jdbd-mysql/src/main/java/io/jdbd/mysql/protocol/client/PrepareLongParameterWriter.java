package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.stmt.LongDataReadException;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.stmt.ParamValue;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

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
                .filter(ParamValue::isLongData)
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
            flux = Flux.create(sink -> {
                if (this.adjutant.inEventLoop()) {
                    sendPathParameterInEventLoop(batchIndex, paramValue, sink);
                } else {
                    this.adjutant.execute(() -> sendPathParameterInEventLoop(batchIndex, paramValue, sink));
                }

            });
        } else if (value instanceof Publisher) {
            flux = new PacketSource(this, batchIndex, paramValue);
        } else {
            MySQLColumnMeta[] paramMetaArray = this.stmtTask.getParameterMetas();
            MySQLType mySQLType = paramMetaArray[paramValue.getIndex()].sqlType;
            flux = Flux.error(MySQLExceptions.createUnsupportedParamTypeError(batchIndex, mySQLType, paramValue));
        }
        return flux;
    }


    private void sendPathParameterInEventLoop(final int batchIndex, final ParamValue paramValue
            , final FluxSink<ByteBuf> sink) {
        if (isTextData(paramValue)) {
            sendTextPathInEventLoop(batchIndex, paramValue, sink);
        } else {
            sendBinaryPathInEventLoop(batchIndex, paramValue, sink);
        }
    }

    private boolean isTextData(final ParamValue paramValue) {
        final boolean textData;
        switch (this.columnMetas[paramValue.getIndex()].sqlType) {
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
                textData = true;
                break;
            default:
                textData = false;
        }
        return textData;
    }


    /**
     * @see #sendPathParameterInEventLoop(int, ParamValue, FluxSink)
     */
    private void sendTextPathInEventLoop(final int batchIndex, final ParamValue paramValue
            , final FluxSink<ByteBuf> sink) {

        ByteBuf packet = null;
        try (FileChannel channel = FileChannel.open((Path) paramValue.getNonNull(), StandardOpenOption.READ)) {

            final Charset clientChart = this.adjutant.charsetClient();
            final Charset textCharset = this.getTextCharset();
            final CharsetEncoder encoder;
            final CharsetDecoder decoder;

            if (textCharset.equals(clientChart)) {
                encoder = null;
                decoder = null;
            } else {
                encoder = clientChart.newEncoder();
                decoder = textCharset.newDecoder();
            }
            final byte[] bufferArray = new byte[2048];

            final int paramIndex = paramValue.getIndex();
            packet = createLongDataPacket(paramIndex, channel.size());
            final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

            while (channel.read(buffer) > 0) {
                buffer.flip();
                if (decoder == null || encoder == null) {
                    packet = writeOneBuffer(packet, paramIndex, bufferArray, buffer.limit(), sink);
                } else {
                    final ByteBuffer decodedBuffer = encoder.encode(decoder.decode(buffer));
                    final byte[] tempBytes = new byte[decodedBuffer.remaining()];
                    decodedBuffer.get(tempBytes);
                    packet = writeOneBuffer(packet, paramIndex, tempBytes, tempBytes.length, sink);
                }
                buffer.clear();
            }
            handleLastPacket(packet, sink::next);
            sink.complete();
        } catch (Throwable e) {
            if (packet != null && packet.refCnt() > 0) {
                packet.release();
            }
            sink.error(createReadError(batchIndex, e, paramValue));
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
            handleLastPacket(packet, sink::next);
            sink.complete();
        } catch (Throwable e) {
            if (packet != null && packet.refCnt() > 0) {
                packet.release();
            }
            sink.error(createReadError(batchIndex, e, paramValue));
        }

    }

    /**
     * @see #sendTextPathInEventLoop(int, ParamValue, FluxSink)
     * @see #sendBinaryPathInEventLoop(int, ParamValue, FluxSink)
     */
    private void handleLastPacket(ByteBuf packet, Consumer<ByteBuf> sink) {
        if (packet.readableBytes() > Packets.HEADER_SIZE + LONG_DATA_PREFIX_SIZE) {
            this.sendLongDataPacket(packet, sink);
        } else {
            packet.release();
        }
    }


    private LongDataReadException createReadError(int batchIndex, Throwable e, ParamValue paramValue) {
        BindValue bindValue;
        if (paramValue instanceof BindValue) {
            bindValue = (BindValue) paramValue;
        } else {
            int paramIndex = paramValue.getIndex();
            bindValue = BindValue.wrap(paramIndex, this.columnMetas[paramIndex].sqlType, paramValue.get());
        }
        return MySQLExceptions.createLongDataReadException(batchIndex, bindValue, e);
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
            this.sendLongDataPacket(packet, sink::next);

            final int resetLength = length - maxWritableBytes;
            packet = createLongDataPacket(paramIndex, resetLength);
            packet.writeBytes(buffer, maxWritableBytes, resetLength);

        }
        return packet;
    }

    private void sendLongDataPacket(final ByteBuf packet, Consumer<ByteBuf> sink) {
        this.stmtTask.resetSequenceId();
        if (packet.readableBytes() < Packets.MAX_PACKET) {
            Packets.writeHeader(packet, this.stmtTask.addAndGetSequenceId());
            sink.accept(packet);
        } else {
            final Iterable<ByteBuf> iterable;
            iterable = Packets.divideBigPacket(packet, this.adjutant.allocator(), this.stmtTask::addAndGetSequenceId);
            for (ByteBuf buffer : iterable) {
                sink.accept(buffer);
            }
        }
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


    /**
     * <p>
     * Get text charset for {@link Publisher} or {@link Path}
     * </p>
     */
    private Charset getTextCharset() {
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
        public void subscribe(final Subscriber<? super ByteBuf> s) {
            try {
                // subscribe upstream
                final PacketSubscription subscription;
                subscription = new PacketSubscription(this.parameterWriter, this.batchIndex, this.paramValue, s);
                ((Publisher<?>) this.paramValue.getNonNull()).subscribe(subscription);

                // invoke downstream  onSubscribe;
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

        private final CharsetDecoder decoder;

        private final CharsetEncoder encoder;

        private final boolean textData;

        private ByteBuf packet;

        private Subscription upstream;

        private boolean terminate;

        private PacketSubscription(PrepareLongParameterWriter parameterWriter, int batchIndex
                , ParamValue paramValue, Subscriber<? super ByteBuf> subscriber) {
            this.parameterWriter = parameterWriter;
            this.batchIndex = batchIndex;
            this.paramValue = paramValue;
            this.subscriber = subscriber;

            final Charset textCharset, clientCharset;
            textCharset = parameterWriter.getTextCharset();
            clientCharset = parameterWriter.adjutant.charsetClient();

            if (parameterWriter.isTextData(paramValue)) {
                this.textData = true;
                if (textCharset.equals(clientCharset)) {
                    this.encoder = null;
                    this.decoder = null;
                } else {
                    this.decoder = textCharset.newDecoder();
                    this.encoder = clientCharset.newEncoder();
                }
            } else {
                this.textData = false;
                this.encoder = null;
                this.decoder = null;
            }

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
                downstreamOnCancel();
            } else {
                this.parameterWriter.adjutant.execute(this::downstreamOnCancel);
            }

        }

        @Override
        public void onSubscribe(Subscription s) {
            this.upstream = s;
        }

        @Override
        public void onNext(final Object item) {
            if (this.parameterWriter.adjutant.inEventLoop()) {
                onNextInEventLoop(item);
            } else {
                this.parameterWriter.adjutant.execute(() -> onNextInEventLoop(item));
            }
        }

        @Override
        public void onError(final Throwable error) {
            if (this.parameterWriter.adjutant.inEventLoop()) {
                onErrorInEventLoop(error);
            } else {
                this.parameterWriter.adjutant.execute(() -> onErrorInEventLoop(error));
            }
        }

        @Override
        public void onComplete() {
            if (this.parameterWriter.adjutant.inEventLoop()) {
                onCompleteInEventLoop();
            } else {
                this.parameterWriter.adjutant.execute(this::onCompleteInEventLoop);
            }
        }


        private void onNextInEventLoop(final Object item) {
            if (this.terminate) {
                return;
            }
            if (item instanceof byte[]) {
                try {
                    if (this.textData) {
                        writeTextData((byte[]) item);
                    } else {
                        writeBinaryData((byte[]) item);
                    }
                } catch (Throwable e) {
                    this.onErrorInEventLoop(e);
                }
            } else {
                String msg = String.format("batch[%s] parameter[%s] Publisher element isn't byte[]."
                        , this.batchIndex, this.paramValue.getIndex());
                this.onErrorInEventLoop(new SQLException(msg));
            }

        }

        private void writeTextData(final byte[] data) throws CharacterCodingException {
            final CharsetDecoder decoder = this.decoder;
            final CharsetEncoder encoder = this.encoder;

            final byte[] encodeArray;
            if (decoder != null && encoder != null) {
                final ByteBuffer buffer;
                buffer = encoder.encode(decoder.decode(ByteBuffer.wrap(data)));
                encodeArray = new byte[buffer.remaining()];
                buffer.get(encodeArray);
            } else {
                encodeArray = data;
            }
            writeBinaryData(encodeArray);
        }

        private void writeBinaryData(final byte[] data) {
            ByteBuf packet = this.packet;
            if (packet == null) {
                packet = this.parameterWriter.createLongDataPacket(this.paramValue.getIndex(), data.length);
                this.packet = packet;
            }

            for (int offset = 0, length; offset < data.length; ) {
                length = Math.min(packet.maxWritableBytes(), data.length - offset);
                packet.writeBytes(data, offset, length);

                offset += length;
                if (packet.maxWritableBytes() > 0) {
                    break;
                }
                this.parameterWriter.sendLongDataPacket(packet, this.subscriber::onNext);

                if (offset == data.length) {
                    this.packet = null;
                    break;
                }
                packet = this.parameterWriter.createLongDataPacket(this.paramValue.getIndex(), data.length - offset);
                this.packet = packet;
            }
        }

        private void onCompleteInEventLoop() {
            final ByteBuf packet = this.packet;
            if (packet != null) {
                this.parameterWriter.handleLastPacket(packet, this.subscriber::onNext);
                this.packet = null;
            }
            this.subscriber.onComplete();
        }


        private void onErrorInEventLoop(final Throwable e) {
            if (this.terminate) {
                return;
            }
            this.terminate = true;
            final ByteBuf packet = this.packet;
            if (packet != null && packet.refCnt() > 0) {
                packet.release();
                this.packet = null;
            }
            cancelSubscribeOnError();
            final Throwable error;
            error = this.parameterWriter.createReadError(this.batchIndex, e, this.paramValue);
            this.subscriber.onError(error);
        }

        private void cancelSubscribeOnError() {
            final Subscription s = this.upstream;
            if (s != null) {
                try {
                    s.cancel();
                } catch (Throwable e) {
                    // Subscription.cannel() shouldn't throw error.
                    LOG.debug("subscription cancel() throw error", e);
                }
            }

        }

        private void downstreamOnCancel() {
            if (this.terminate) {
                return;
            }
            this.terminate = true;
            final ByteBuf packet = this.packet;
            if (packet != null && packet.refCnt() > 0) {
                packet.release();
                this.packet = null;
            }

        }

    }


}
