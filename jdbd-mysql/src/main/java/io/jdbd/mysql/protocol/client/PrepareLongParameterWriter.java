package io.jdbd.mysql.protocol.client;

import io.jdbd.BindParameterException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.BindValue;
import io.jdbd.mysql.protocol.ClientConstants;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.vendor.LongParameterException;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html">Protocol::COM_STMT_SEND_LONG_DATA</a>
 */
final class PrepareLongParameterWriter implements PrepareExecuteCommandWriter.LongParameterWriter {

    private static final int LONG_DATA_PREFIX_SIZE = 7;

    /**
     * chunk can't send multi packet,avoid long data error,handle error difficulty.
     */
    private static final int MAX_CHUNK_SIZE = PacketUtils.MAX_PAYLOAD - LONG_DATA_PREFIX_SIZE - 1;

    private static final int MIN_CHUNK_SIZE = ClientConstants.BUFFER_LENGTH;

    private final StatementTask statementTask;

    private final int statementId;

    private final ClientProtocolAdjutant adjutant;

    private final Properties properties;

    private final int blobSendChunkSize;

    private final int maxPacketCapacity;


    PrepareLongParameterWriter(final StatementTask statementTask) {
        this.statementTask = statementTask;

        this.statementId = statementTask.obtainStatementId();
        this.adjutant = statementTask.obtainAdjutant();
        this.blobSendChunkSize = obtainBlobSendChunkSize();

        this.properties = this.adjutant.obtainHostInfo().getProperties();
        this.maxPacketCapacity = PacketUtils.HEADER_SIZE + LONG_DATA_PREFIX_SIZE + blobSendChunkSize;

    }


    @Override
    public final Flux<ByteBuf> write(List<BindValue> valueList) {
        return Flux.fromIterable(valueList)
                .filter(BindValue::isLongData)
                .flatMap(this::sendLongData);
    }


    /*################################## blow private method ##################################*/

    /**
     * @see #write(List)
     */
    private Flux<ByteBuf> sendLongData(final BindValue bindValue) {
        final Object value = bindValue.getRequiredValue();

        Flux<ByteBuf> flux;
        if (value instanceof byte[]) {
            flux = sendByteArrayParameter(bindValue.getParamIndex(), (byte[]) value);
        } else if (value instanceof InputStream) {
            flux = sendInputStreamParameter(bindValue.getParamIndex(), (InputStream) value);
        } else if (value instanceof ReadableByteChannel) {
            flux = sendReadByteChannelParameter(bindValue.getParamIndex(), (ReadableByteChannel) value);
        } else if (value instanceof Reader) {
            flux = sendReaderParameter(bindValue.getParamIndex(), (Reader) value);
        } else if (value instanceof char[]) {
            flux = sendCharArrayParameter(bindValue.getParamIndex(), (char[]) value);
        } else if (value instanceof String) {
            flux = sendStringParameter(bindValue.getParamIndex(), (String) value);
        } else if (value instanceof Path) {
            flux = sendPathParameter(bindValue.getParamIndex(), (Path) value);
        } else if (value instanceof Publisher) {
            flux = sendPublisher(bindValue.getParamIndex(), (Publisher<?>) value);
        } else {
            flux = Flux.error(new LongParameterException(bindValue.getParamIndex()
                    , "Bind parameter[%s] type[%s] not support."
                    , bindValue.getParamIndex(), value.getClass().getName()));
        }

        return flux;
    }

    /**
     * @see #sendLongData(BindValue)
     */
    private Flux<ByteBuf> sendByteArrayParameter(final int paramIndex, final byte[] input) {
        return Flux.create(sink -> {

            ByteBuf packet = createLongDataPacket(paramIndex, Math.min(this.blobSendChunkSize, input.length));
            packet = writeByteArray(packet, sink, paramIndex, input, input.length);
            publishLastPacket(packet, sink);

        });

    }

    /**
     * @see #sendLongData(BindValue)
     */
    private Flux<ByteBuf> sendCharArrayParameter(final int paramIndex, final char[] input) {
        return Flux.create(sink -> {
            ByteBuf packet = createLongDataPacket(paramIndex, Math.min(this.blobSendChunkSize, input.length));

            packet = writeCharArray(packet, sink, paramIndex, input, input.length);
            publishLastPacket(packet, sink);

        });
    }

    /**
     * @see #sendLongData(BindValue)
     */
    private Flux<ByteBuf> sendStringParameter(final int paramIndex, final String string) {
        return Flux.create(sink -> {
            final long byteLength = (long) string.length() * this.adjutant.obtainMaxBytesPerCharClient();
            ByteBuf packet;
            if (byteLength > Integer.MAX_VALUE) {
                packet = createLongDataPacket(paramIndex, this.blobSendChunkSize);
                char[] charArray = string.toCharArray();
                packet = writeCharArray(packet, sink, paramIndex, charArray, charArray.length);
            } else {
                packet = createLongDataPacket(paramIndex, Math.min(this.blobSendChunkSize, (int) byteLength));
                byte[] bytes = string.getBytes(this.adjutant.obtainCharsetClient());
                packet = writeByteArray(packet, sink, paramIndex, bytes, bytes.length);
            }

            publishLastPacket(packet, sink);


        });
    }


    /**
     * @see #sendLongData(BindValue)
     */
    private Flux<ByteBuf> sendInputStreamParameter(final int paramIndex, InputStream input) {
        return Flux.create(sink -> writeInputStream(paramIndex, input, sink
                , this.properties.getOrDefault(PropertyKey.autoClosePStmtStreams, Boolean.class)));
    }

    /**
     * @see #sendLongData(BindValue)
     */
    private Flux<ByteBuf> sendReadByteChannelParameter(final int paramIndex, final ReadableByteChannel input) {
        return Flux.create(sink -> {
            ByteBuf packet = null;
            try {
                packet = createLongDataPacket(paramIndex, ClientConstants.BUFFER_LENGTH);
                final ByteBuffer buffer = ByteBuffer.allocate(ClientConstants.BUFFER_LENGTH);
                final int maxPacketCapacity = this.maxPacketCapacity;
                while (input.read(buffer) > 0) {
                    packet.writeBytes(buffer);
                    if (packet.readableBytes() >= maxPacketCapacity) {
                        packet = publishLongDataPacketAndCut(packet, paramIndex, sink);
                    }
                    buffer.clear();
                }

                publishLastPacket(packet, sink);
            } catch (IOException e) {
                packet.release();
                publishLonDataReadException(sink, e, paramIndex, input);
            } finally {
                autoClose(input, paramIndex, sink);
            }


        });
    }

    /**
     * @see #sendLongData(BindValue)
     */
    private Flux<ByteBuf> sendReaderParameter(final int paramIndex, final Reader input) {
        return Flux.create(sink -> {
            ByteBuf packet = null;
            try {
                final Charset readerCharset = obtainReaderCharset();
                final CharBuffer charBuffer = CharBuffer.allocate(ClientConstants.BUFFER_LENGTH >> 1);
                final int maxPacketCapacity = this.maxPacketCapacity;
                packet = createLongDataPacket(paramIndex, ClientConstants.BUFFER_LENGTH);

                ByteBuffer byteBuffer;
                while (input.read(charBuffer) > 0) { //1. read char stream
                    byteBuffer = readerCharset.encode(charBuffer);   //2. encode char to byte
                    packet.writeBytes(byteBuffer); // 3.write byte to packet.
                    if (packet.readableBytes() >= maxPacketCapacity) {
                        packet = publishLongDataPacketAndCut(packet, paramIndex, sink);
                    }
                    charBuffer.clear(); // 4. clear charBuffer
                }

                publishLastPacket(packet, sink);

            } catch (IOException e) {
                packet.release();
                publishLonDataReadException(sink, e, paramIndex, input);
            } finally {
                autoClose(input, paramIndex, sink);
            }


        });
    }


    private Flux<ByteBuf> sendPathParameter(final int paramIndex, final Path path) {
        return Flux.create(sink -> {
            try (InputStream input = Files.newInputStream(path, StandardOpenOption.READ)) {
                writeInputStream(paramIndex, input, sink, false);
            } catch (IOException e) {
                publishLonDataReadException(sink, e, paramIndex, path);
            }

        });
    }


    private Flux<ByteBuf> sendPublisher(final int paramIndex, Publisher<?> input) {
        return Flux.create(sink -> Flux.from(input)
                .subscribeWith(new PublisherLongDataSubscriber(sink, paramIndex)));
    }


    private ByteBuf createLongDataPacket(final int parameterIndex, final int chunkSize) {
        int payloadCapacity;
        if (chunkSize == 0) {
            payloadCapacity = LONG_DATA_PREFIX_SIZE;
        } else if (chunkSize < 1024) {
            payloadCapacity = 1024;
        } else {
            payloadCapacity = LONG_DATA_PREFIX_SIZE + Math.min(this.blobSendChunkSize, chunkSize);
        }
        ByteBuf packetBuffer = this.adjutant.alloc().buffer(payloadCapacity, PacketUtils.MAX_PAYLOAD << 2);

        packetBuffer.writeByte(PacketUtils.COM_STMT_SEND_LONG_DATA); //status
        PacketUtils.writeInt4(packetBuffer, this.statementId); //statement_id
        PacketUtils.writeInt2(packetBuffer, parameterIndex);//param_id
        return packetBuffer;

    }

    private int obtainBlobSendChunkSize() {
        int packetChunkSize = this.properties.getOrDefault(PropertyKey.blobSendChunkSize, Integer.class);
        if (packetChunkSize < MIN_CHUNK_SIZE) {
            packetChunkSize = MIN_CHUNK_SIZE;
        } else if (packetChunkSize > MAX_CHUNK_SIZE) {
            packetChunkSize = MAX_CHUNK_SIZE;
        }
        return packetChunkSize;
    }

    /**
     * @see #sendInputStreamParameter(int, InputStream)
     */
    private void writeInputStream(final int parameterIndex, final InputStream input, FluxSink<ByteBuf> sink
            , final boolean autoClose) {
        ByteBuf packet = null;
        try {
            packet = createLongDataPacket(parameterIndex, ClientConstants.BUFFER_LENGTH);
            final byte[] buffer = new byte[ClientConstants.BUFFER_LENGTH];
            final int maxPacketCapacity = this.maxPacketCapacity;
            for (int length; (length = input.read(buffer)) > 0; ) {
                packet.writeBytes(buffer, 0, length);
                if (packet.readableBytes() >= maxPacketCapacity) {
                    packet = publishLongDataPacketAndCut(packet, parameterIndex, sink);
                }

            }

            publishLastPacket(packet, sink);
        } catch (Throwable e) {
            if (packet != null) {
                packet.release();
            }
            publishLonDataReadException(sink, e, parameterIndex, input);
        } finally {
            if (autoClose) {
                try {
                    input.close();
                } catch (IOException e) {
                    sink.error(new BindParameterException(
                            e, parameterIndex, "Bind parameter[%s] %s close failure.", input.getClass().getName()));
                }
            }
        }
    }


    /**
     * @see #sendCharArrayParameter(int, char[])
     */
    private ByteBuf writeCharArray(final ByteBuf packetBuffer, FluxSink<ByteBuf> sink
            , final int paramIndex, final char[] input, final int arrayLength) {

        if (arrayLength < 0 || arrayLength > input.length) {
            throw new IllegalArgumentException("arrayLength error");
        }

        final Charset clientCharset = this.adjutant.obtainCharsetClient();
        final int bufferLength = ClientConstants.BUFFER_LENGTH / this.adjutant.obtainMaxBytesPerCharClient();
        final int maxPacketCapacity = this.maxPacketCapacity;
        ByteBuf packet = packetBuffer;

        final CharBuffer charBuffer = CharBuffer.allocate(Math.min(bufferLength, arrayLength));
        ByteBuffer byteBuffer;
        for (int offset = 0, length; offset < arrayLength; ) {

            length = Math.min(charBuffer.capacity(), arrayLength - offset);

            // 1. read char array
            charBuffer.put(input, offset, length);
            // 2. encode
            byteBuffer = clientCharset.encode(charBuffer);
            // 3. write to packet
            packet.writeBytes(byteBuffer);

            offset += length;

            if (packet.readableBytes() >= maxPacketCapacity) {
                packet = publishLongDataPacketAndCut(packet, paramIndex, sink);
            }
            charBuffer.clear();
        }
        return packet;
    }

    /**
     * @see #sendByteArrayParameter(int, byte[])
     */
    private ByteBuf writeByteArray(final ByteBuf packetBuffer, FluxSink<ByteBuf> sink
            , final int paramIndex, final byte[] input, final int arrayLength) {

        if (arrayLength < 0 || arrayLength > input.length) {
            throw new IllegalArgumentException("arrayLength error");
        }
        final int maxPacketCapacity = this.maxPacketCapacity;
        ByteBuf packet = packetBuffer;
        for (int offset = 0, length; offset < arrayLength; ) {

            length = Math.min(ClientConstants.BUFFER_LENGTH, arrayLength - offset);

            //  write to packet
            packet.writeBytes(input, offset, length);

            offset += length;

            if (packet.readableBytes() >= maxPacketCapacity) {
                packet = publishLongDataPacketAndCut(packet, paramIndex, sink);
            }
        }
        return packet;
    }


    /**
     * @see #writeInputStream(int, InputStream, FluxSink, boolean)
     * @see #sendByteArrayParameter(int, byte[])
     */
    private ByteBuf publishLongDataPacketAndCut(final ByteBuf bigPacket, final int paramIndex
            , final FluxSink<ByteBuf> sink) {

        final int maxPacketCapacity = this.maxPacketCapacity;
        ByteBuf packet;
        if (bigPacket.readableBytes() >= maxPacketCapacity) {
            packet = bigPacket.readRetainedSlice(maxPacketCapacity);
            PacketUtils.writePacketHeader(packet, this.statementTask.addAndGetSequenceId());
            sink.next(packet);
        }
        packet = createLongDataPacket(paramIndex, bigPacket.readableBytes());
        packet.writeBytes(bigPacket);
        bigPacket.release();
        return packet;
    }

    /**
     * @see #writeInputStream(int, InputStream, FluxSink, boolean)
     * @see #sendByteArrayParameter(int, byte[])
     */
    private void publishLastPacket(final ByteBuf packet, final FluxSink<ByteBuf> sink) {
        if (packet.readableBytes() > (PacketUtils.HEADER_SIZE + LONG_DATA_PREFIX_SIZE)) {
            PacketUtils.publishBigPacket(packet, sink, this.statementTask::addAndGetSequenceId
                    , this.adjutant::createByteBuffer, true);
        } else {
            packet.release();
        }
    }


    /**
     * @see #writeInputStream(int, InputStream, FluxSink, boolean)
     * @see ComPreparedTask#internalError(Throwable)
     */
    private void publishLonDataReadException(FluxSink<ByteBuf> sink, Throwable cause
            , int parameterIndex, final @Nullable Object input) {
        Class<?> javaType = input == null ? Object.class : input.getClass();
        LongParameterException e = new LongParameterException(cause, parameterIndex
                , "Bind parameter[%s](%s) read error.", parameterIndex, javaType.getName());
        sink.error(e);
    }

    /**
     * @see #writeInputStream(int, InputStream, FluxSink, boolean)
     */
    private void autoClose(final Closeable input, final int parameterIndex, final FluxSink<ByteBuf> sink) {
        if (this.properties.getOrDefault(PropertyKey.autoClosePStmtStreams, Boolean.class)) {
            try {
                input.close();
            } catch (IOException e) {
                sink.error(new BindParameterException(
                        e, parameterIndex, "Bind parameter[%s] %s close failure.", input.getClass().getName()));
            }
        }
    }

    private Charset obtainReaderCharset() {
        Charset charset = this.properties.getOrDefault(PropertyKey.clobCharacterEncoding, Charset.class);
        if (charset == null) {
            charset = this.adjutant.obtainCharsetClient();
        }
        return charset;
    }




    /*################################## blow private instance inner class ##################################*/

    private final class PublisherLongDataSubscriber implements CoreSubscriber<Object> {

        private final FluxSink<ByteBuf> sink;

        private final int parameterIndex;

        ByteBuf packet;

        private PublisherLongDataSubscriber(FluxSink<ByteBuf> sink, int parameterIndex) {
            this.sink = sink;
            this.parameterIndex = parameterIndex;
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(final Object data) {
            if (data instanceof byte[]) {
                byte[] bytes = (byte[]) data;
                writeBytes(bytes, bytes.length);
            } else if (data instanceof ByteBuffer) {
                writeByteBuffer((ByteBuffer) data);
            } else if (data instanceof ByteBuf) {
                writeByteBuf((ByteBuf) data);
            } else {
                BindParameterException e = new BindParameterException(this.parameterIndex
                        , "Publisher<%s> is supported.", data.getClass().getName());
                PrepareLongParameterWriter.this.publishLonDataReadException(this.sink, e, this.parameterIndex, data);
            }

        }


        @Override
        public void onError(Throwable t) {
            ByteBuf packet = this.packet;
            if (packet != null) {
                packet.release();
            }
            PrepareLongParameterWriter.this.publishLonDataReadException(this.sink, t, this.parameterIndex, null);
        }

        @Override
        public void onComplete() {
            ByteBuf packet = this.packet;
            if (packet != null) {
                PrepareLongParameterWriter.this.publishLastPacket(packet, this.sink);
                this.packet = null;
            }
        }

        private void writeBytes(final byte[] dataBytes, final int arrayLength) {
            final int blobSendChunkSize = PrepareLongParameterWriter.this.blobSendChunkSize;
            int length = Math.min(blobSendChunkSize, arrayLength);

            ByteBuf packet = this.packet;
            if (packet == null) {
                packet = createLongDataPacket(this.parameterIndex, length);
            }

            final int maxPacketCapacity = PrepareLongParameterWriter.this.maxPacketCapacity;

            for (int offset = 0; offset < arrayLength; ) {
                packet.writeBytes(dataBytes, offset, length);
                offset += length;
                length = Math.min(blobSendChunkSize, arrayLength - offset);

                if (packet.readableBytes() >= maxPacketCapacity) {
                    packet = publishLongDataPacketAndCut(packet, this.parameterIndex, this.sink);
                }

            }

            this.packet = packet;

        }

        private void writeByteBuffer(final ByteBuffer dataBuffer) {

            if (dataBuffer.remaining() < PrepareLongParameterWriter.this.blobSendChunkSize) {
                ByteBuf packet = this.packet;
                if (packet == null) {
                    packet = createLongDataPacket(this.parameterIndex, dataBuffer.remaining());
                }
                packet.writeBytes(dataBuffer);
                if (packet.readableBytes() >= PrepareLongParameterWriter.this.maxPacketCapacity) {
                    packet = publishLongDataPacketAndCut(packet, this.parameterIndex, this.sink);
                }
                this.packet = packet;
            } else {
                final byte[] bufferArray = new byte[Math.min(dataBuffer.remaining(), ClientConstants.BUFFER_LENGTH)];
                for (int length; dataBuffer.hasRemaining(); ) {
                    length = Math.min(bufferArray.length, dataBuffer.remaining());
                    dataBuffer.get(bufferArray, 0, length);
                    writeBytes(bufferArray, length);
                }
            }

        }


        private void writeByteBuf(final ByteBuf dataBuffer) {
            final int blobSendChunkSize = PrepareLongParameterWriter.this.blobSendChunkSize;
            ByteBuf packet = this.packet;
            if (packet == null) {
                packet = createLongDataPacket(this.parameterIndex
                        , Math.min(dataBuffer.readableBytes(), blobSendChunkSize));
            }
            final int maxPacketCapacity = PrepareLongParameterWriter.this.maxPacketCapacity;

            for (int length; dataBuffer.isReadable(); ) {
                length = Math.min(dataBuffer.readableBytes(), blobSendChunkSize);
                packet.writeBytes(dataBuffer, length);

                if (packet.readableBytes() >= maxPacketCapacity) {
                    packet = publishLongDataPacketAndCut(packet, this.parameterIndex, this.sink);
                }


            }
            this.packet = packet;
        }


    }


}
