package io.jdbd.mysql.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.ClientConstants;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.stmt.LongDataReadException;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.stmt.ParamValue;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html">Protocol::COM_STMT_SEND_LONG_DATA</a>
 */
final class PrepareLongParameterWriter implements PrepareExecuteCommandWriter.LongParameterWriter {

    private static final Logger LOG = LoggerFactory.getLogger(PrepareLongParameterWriter.class);

    private static final int LONG_DATA_PREFIX_SIZE = 7;

    /**
     * chunk can't send multi packet,avoid long data error,handle error difficulty.
     */
    private static final int MAX_CHUNK_SIZE = Packets.MAX_PAYLOAD - LONG_DATA_PREFIX_SIZE - 1;

    private static final int MIN_CHUNK_SIZE = ClientConstants.BUFFER_LENGTH;

    private final StatementTask statementTask;

    private final int statementId;

    private final ClientProtocolAdjutant adjutant;

    private final Properties<PropertyKey> properties;

    private final int blobSendChunkSize;

    private final int maxPacket;


    PrepareLongParameterWriter(final StatementTask statementTask) {
        this.statementTask = statementTask;
        this.statementId = statementTask.obtainStatementId();
        this.adjutant = statementTask.obtainAdjutant();
        this.properties = this.adjutant.obtainHostInfo().getProperties();

        this.blobSendChunkSize = obtainBlobSendChunkSize();
        this.maxPacket = Packets.HEADER_SIZE + blobSendChunkSize;

    }


    @Override
    public final Flux<ByteBuf> write(final int stmtIndex, List<? extends ParamValue> valueList) {
        return Flux.fromIterable(valueList)
                .filter(ParamValue::isLongData)
                .flatMap(paramValue -> sendLongData(stmtIndex, paramValue));
    }


    /*################################## blow private method ##################################*/


    /**
     * @see #write(int, List)
     */
    private Flux<ByteBuf> sendLongData(final int stmtIndex, final ParamValue paramValue) {
        final Object value = paramValue.getNonNull();

        final Flux<ByteBuf> flux;
        if (value instanceof byte[]) {
            flux = sendByteArrayParameter(paramValue.getIndex(), (byte[]) value);
        } else if (value instanceof String) {
            flux = sendStringParameter(paramValue.getIndex(), (String) value);
        } else if (value instanceof Path) {
            flux = sendPathParameter(stmtIndex, paramValue.getIndex(), (Path) value);
        } else if (value instanceof Publisher) {
            flux = sendPublisher(stmtIndex, paramValue.getIndex(), (Publisher<?>) value);
        } else {
            MySQLColumnMeta[] paramMetaArray = this.statementTask.obtainParameterMetas();
            MySQLType mySQLType = paramMetaArray[paramValue.getIndex()].mysqlType;
            flux = Flux.error(MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, mySQLType, paramValue));
        }
        return flux;
    }

    /**
     * @see #sendLongData(int, ParamValue)
     */
    private Flux<ByteBuf> sendByteArrayParameter(final int paramIndex, final byte[] input) {
        return Flux.create(sink -> {
            ByteBuf packet = createLongDataPacket(paramIndex, input.length);

            packet = writeByteArray(packet, sink, paramIndex, input, input.length);
            Packets.writePacketHeader(packet, this.statementTask.safelyAddAndGetSequenceId());
            sink.next(packet);

            sink.complete();
        });

    }


    /**
     * @see #sendLongData(int, ParamValue)
     */
    private Flux<ByteBuf> sendStringParameter(final int paramIndex, final String string) {
        return Flux.create(sink -> {
            ByteBuf packet;
            if (string.length() < (1 << 26)) {
                packet = createLongDataPacket(paramIndex, string.length());
                byte[] bytes = string.getBytes(obtainCharset(paramIndex));
                packet = writeByteArray(packet, sink, paramIndex, bytes, bytes.length);
            } else {
                packet = createLongDataPacket(paramIndex, this.blobSendChunkSize);
                char[] charArray = string.toCharArray();
                packet = writeCharArray(packet, sink, paramIndex, charArray, charArray.length);
            }
            Packets.writePacketHeader(packet, this.statementTask.safelyAddAndGetSequenceId());
            sink.next(packet);

            sink.complete();
        });
    }


    /**
     * @see #sendLongData(int, ParamValue)
     */
    private Flux<ByteBuf> sendPathParameter(final int stmtIndex, final int paramIndex, final Path path) {
        return Flux.create(sink -> {

            try (InputStream input = Files.newInputStream(path, StandardOpenOption.READ)) {
                writeInputStream(stmtIndex, paramIndex, input, sink);
            } catch (Throwable e) {
                publishLonDataReadException(sink, e, stmtIndex, paramIndex, path);
            }

        });
    }


    /**
     * @see #sendLongData(int, ParamValue)
     */
    private Flux<ByteBuf> sendPublisher(final int stmtIndex, final int paramIndex, final Publisher<?> publisher) {
        return Flux.create(sink -> {
            if (LOG.isTraceEnabled()) {
                LOG.trace("handle publisher parameter.");
            }
            Flux.from(publisher)
                    .map(ByteBuffer.class::cast)
                    .subscribe(new ByteBufferSubscriber(sink, stmtIndex, paramIndex));
        });

    }


    private ByteBuf createLongDataPacket(final int parameterIndex, final int chunkSize) {
        final int payloadCapacity;
        if (chunkSize == 0) {
            payloadCapacity = LONG_DATA_PREFIX_SIZE;
        } else if (chunkSize < 1024) {
            payloadCapacity = 1024;
        } else {
            payloadCapacity = Math.min(this.blobSendChunkSize, chunkSize);
        }
        ByteBuf packet = this.adjutant.allocator()
                .buffer(Packets.HEADER_SIZE + payloadCapacity, Packets.MAX_PAYLOAD << 1);

        packet.writeZero(Packets.HEADER_SIZE);
        packet.writeByte(Packets.COM_STMT_SEND_LONG_DATA); //status
        Packets.writeInt4(packet, this.statementId); //statement_id
        Packets.writeInt2(packet, parameterIndex);//param_id
        return packet;

    }

    private int obtainBlobSendChunkSize() {
        int chunkSize = this.properties.getOrDefault(PropertyKey.blobSendChunkSize, Integer.class);
        final int maxChunkSize = Math.min(this.adjutant.obtainHostInfo().maxAllowedPayload(), MAX_CHUNK_SIZE);
        if (chunkSize < MIN_CHUNK_SIZE) {
            chunkSize = MIN_CHUNK_SIZE;
        } else if (chunkSize > maxChunkSize) {
            chunkSize = maxChunkSize;
        }
        return chunkSize;
    }

    /**
     * @see #sendPathParameter(int, int, Path)
     */
    private void writeInputStream(final int stmtIndex, final int parameterIndex, final InputStream input
            , FluxSink<ByteBuf> sink) {
        ByteBuf packet = null;
        try {
            packet = createLongDataPacket(parameterIndex, ClientConstants.BUFFER_LENGTH);
            final byte[] buffer = new byte[ClientConstants.BUFFER_LENGTH];
            final int maxPacket = this.maxPacket;
            for (int length; (length = input.read(buffer)) > 0; ) {
                if (packet.readableBytes() == maxPacket) {
                    Packets.writePacketHeader(packet, this.statementTask.safelyAddAndGetSequenceId());
                    sink.next(packet);

                    packet = createLongDataPacket(parameterIndex, ClientConstants.BUFFER_LENGTH);
                }
                packet.writeBytes(buffer, 0, length);
            }
            Packets.writePacketHeader(packet, this.statementTask.safelyAddAndGetSequenceId());
            sink.next(packet);

            sink.complete();
        } catch (Throwable e) {
            if (packet != null) {
                packet.release();
            }
            publishLonDataReadException(sink, e, stmtIndex, parameterIndex, input);
        }


    }


    /**
     * @see #sendStringParameter(int, String)
     */
    private ByteBuf writeCharArray(ByteBuf packet, FluxSink<ByteBuf> sink
            , final int paramIndex, final char[] charArray, final int arrayLength) {

        if (arrayLength < 0 || arrayLength > charArray.length) {
            throw new IllegalArgumentException("arrayLength error");
        }

        final Charset charset;
        if (this.statementTask.obtainParameterMetas()[paramIndex].mysqlType.isLongString()) {
            charset = obtainClobCharset();
        } else {
            charset = this.adjutant.obtainCharsetClient();
        }
        final int maxPacket = this.maxPacket;
        final CharBuffer charBuffer = CharBuffer.allocate(ClientConstants.BUFFER_LENGTH);
        ByteBuffer byteBuffer;
        byte[] byteArray;
        for (int offset = 0, length; offset < arrayLength; offset += length) {
            if (packet.readableBytes() == maxPacket) {
                Packets.writePacketHeader(packet, this.statementTask.safelyAddAndGetSequenceId());
                sink.next(packet);
                packet = createLongDataPacket(paramIndex, arrayLength - offset);
            }
            length = Math.min(ClientConstants.BUFFER_LENGTH, arrayLength - offset);
            // 1. read char array
            charBuffer.put(charArray, offset, length);
            charBuffer.flip();
            // 2. encode
            byteBuffer = charset.encode(charBuffer);
            // 3. write to packet
            byteArray = new byte[byteBuffer.remaining()];
            byteBuffer.get(byteArray);
            packet = writeByteArray(packet, sink, paramIndex, byteArray, byteArray.length);
            charBuffer.clear();
        }
        return packet;
    }

    /**
     * @return the pack that {@link ByteBuf#readableBytes()} less than {@link #maxPacket}.
     * @see #sendByteArrayParameter(int, byte[])
     */
    private ByteBuf writeByteArray(ByteBuf packet, FluxSink<ByteBuf> sink, final int paramIndex
            , final byte[] input, final int arrayLength) {

        if (arrayLength < 0 || arrayLength > input.length) {
            throw new IllegalArgumentException("arrayLength error");
        }
        final int maxPacket = this.maxPacket;
        for (int offset = 0, length; offset < arrayLength; offset += length) {

            if (packet.readableBytes() == maxPacket) {
                Packets.writePacketHeader(packet, this.statementTask.safelyAddAndGetSequenceId());
                sink.next(packet);
                packet = createLongDataPacket(paramIndex, arrayLength - offset);
            }
            length = Math.min(maxPacket - packet.readableBytes(), arrayLength - offset);
            //  write to packet
            packet.writeBytes(input, offset, length);
        }
        return packet;
    }


    /**
     * @see #writeInputStream(int, int, InputStream, FluxSink)
     * @see ComPreparedTask#onError(Throwable)
     */
    private void publishLonDataReadException(FluxSink<ByteBuf> sink, Throwable cause, final int stmtIndex
            , int parameterIndex, final @Nullable Object input) {
        Class<?> javaType = input == null ? Object.class : input.getClass();
        LongDataReadException e;
        if (stmtIndex < 0) {
            String message = String.format("Bind parameter[%s](%s) read error.", parameterIndex, javaType.getName());
            e = new LongDataReadException(cause, message);
        } else {
            String message = String.format("Parameter Group[%s] Bind parameter[%s](%s) read error."
                    , stmtIndex, parameterIndex, javaType.getName());
            e = new LongDataReadException(cause, message);
        }
        sink.error(e);
    }


    private Charset obtainCharset(final int paramIndex) {
        final Charset charset;
        if (this.statementTask.obtainParameterMetas()[paramIndex].mysqlType.isLongString()) {
            charset = obtainClobCharset();
        } else {
            charset = this.adjutant.obtainCharsetClient();
        }
        return charset;
    }

    /**
     * @see #writeCharArray(ByteBuf, FluxSink, int, char[], int)
     */
    private Charset obtainClobCharset() {
        Charset charset = this.properties.get(PropertyKey.clobCharacterEncoding, Charset.class);
        if (charset == null) {
            charset = this.adjutant.obtainCharsetClient();
        }
        return charset;
    }


    /*################################## blow private instance inner class ##################################*/

    /**
     * @see #sendPublisher(int, int, Publisher)
     */
    private final class ByteBufferSubscriber implements CoreSubscriber<ByteBuffer> {

        private final FluxSink<ByteBuf> sink;

        private final int stmtIndex;

        private final int parameterIndex;

        private ByteBuf packet;

        private ByteBufferSubscriber(FluxSink<ByteBuf> sink, int stmtIndex, int parameterIndex) {
            this.sink = sink;
            this.stmtIndex = stmtIndex;
            this.parameterIndex = parameterIndex;
            this.packet = createLongDataPacket(this.parameterIndex, ClientConstants.BUFFER_LENGTH);
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer buffer) {
            final byte[] bytes;
            final int length = buffer.remaining();
            if (buffer.hasArray() && buffer.position() == 0) {
                bytes = buffer.array();
            } else {
                bytes = new byte[length];
                buffer.get(bytes);
            }
            this.packet = writeByteArray(this.packet, this.sink, this.parameterIndex, bytes, length);
        }

        @Override
        public void onError(Throwable t) {
            ByteBuf packet = this.packet;
            if (packet != null) {
                packet.release();
                this.packet = null;
            }
            final String message;
            if (this.stmtIndex < 0) {
                message = String.format("Bind parameter[%s] Publisher read error.", this.parameterIndex);
            } else {
                message = String.format("Parameter Group[%s] Bind parameter[%s] Publisher read error,\nbecause %s"
                        , this.stmtIndex, this.parameterIndex, t.getMessage());
            }
            this.sink.error(new LongDataReadException(t, message));
        }

        @Override
        public void onComplete() {
            this.sink.complete();
        }


    }





}
