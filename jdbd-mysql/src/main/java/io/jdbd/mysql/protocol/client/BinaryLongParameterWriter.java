package io.jdbd.mysql.protocol.client;

import io.jdbd.BindParameterException;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

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
import java.util.function.Supplier;

final class BinaryLongParameterWriter implements BinaryStatementCommandWriter.LongParameterWriter {

    private static final int LONG_DATA_PREFIX_SIZE = 7;

    private static final int MAX_CHUNK_SIZE = PacketUtils.MAX_PAYLOAD - LONG_DATA_PREFIX_SIZE;

    private static final int BUFFER_LENGTH = 8192;

    private static final int MIN_CHUNK_SIZE = BUFFER_LENGTH;

    final int statementId;

    final ClientProtocolAdjutant adjutant;

    final Properties properties;

    final Supplier<Integer> sequenceIdSupplier;

    final int blobSendChunkSize;

    final int maxPacketCapacity;

    BinaryLongParameterWriter(int statementId, ClientProtocolAdjutant adjutant, Supplier<Integer> sequenceIdSupplier) {
        this.statementId = statementId;
        this.adjutant = adjutant;
        this.sequenceIdSupplier = sequenceIdSupplier;
        this.blobSendChunkSize = obtainBlobSendChunkSize();

        this.properties = adjutant.obtainHostInfo().getProperties();
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
            flux = sendByteArray(bindValue.getParamIndex(), (byte[]) value);
        } else if (value instanceof InputStream) {
            flux = sendInputStream(bindValue.getParamIndex(), (InputStream) value);
        } else if (value instanceof ReadableByteChannel) {
            flux = sendReadByteChannel(bindValue.getParamIndex(), (ReadableByteChannel) value);
        } else if (value instanceof Reader) {
            flux = sendReader(bindValue.getParamIndex(), (Reader) value);
        } else if (value instanceof Path) {
            flux = sendPath(bindValue.getParamIndex(), (Path) value);
        } else if (value instanceof Publisher) {
            flux = sendPublisher(bindValue.getParamIndex(), (Publisher<?>) value);
        } else {
            flux = Flux.error(new BindParameterException(
                    String.format("Bind parameter[%s] type[%s] not support."
                            , bindValue.getParamIndex(), value.getClass().getName()), bindValue.getParamIndex()));
        }
        return flux;
    }

    private Flux<ByteBuf> sendByteArray(final int paramIndex, final byte[] input) {
        return Flux.create(sink -> {
            if (input.length < this.blobSendChunkSize && this.blobSendChunkSize < MAX_CHUNK_SIZE) {
                ByteBuf packetBuffer = createLongDataPacket(paramIndex, input.length);
                packetBuffer.writeBytes(input);
                PacketUtils.writePacketHeader(packetBuffer, this.sequenceIdSupplier.get());
                sink.next(packetBuffer);
            } else {
                for (int offset = 0, chunkSize = this.blobSendChunkSize, restLength = input.length; restLength > 0; ) {
                    ByteBuf packetBuffer = createLongDataPacket(paramIndex, chunkSize);
                    packetBuffer.writeBytes(input, offset, chunkSize);
                    PacketUtils.writePacketHeader(packetBuffer, this.sequenceIdSupplier.get());
                    sink.next(packetBuffer);

                    offset += chunkSize;
                    restLength = input.length - offset;
                    chunkSize = Math.min(chunkSize, restLength);
                }
                if (input.length % MAX_CHUNK_SIZE == 0) {
                    sink.next(createLongDataPacket(paramIndex, 0));
                }
            }
        });

    }

    private Flux<ByteBuf> sendInputStream(final int paramIndex, InputStream input) {
        return Flux.create(sink -> writeInputStreamParameter(paramIndex, input, sink));
    }

    private Flux<ByteBuf> sendReadByteChannel(final int paramIndex, ReadableByteChannel input) {
        return Flux.create(sink -> {
            ByteBuf packetBuffer = null;
            try {
                packetBuffer = createLongDataPacket(paramIndex, BUFFER_LENGTH << 1);
                final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_LENGTH);
                while (input.read(buffer) > 0) {
                    packetBuffer = writeByteBufferToBlobPacket(buffer, paramIndex, packetBuffer, sink);
                }
                if (hasBlobData(packetBuffer)) {
                    PacketUtils.writePacketHeader(packetBuffer, this.sequenceIdSupplier.get()); // write header
                    sink.next(packetBuffer); // send packet
                } else {
                    packetBuffer.release();
                }

            } catch (IOException e) {
                packetBuffer.release();
                sink.error(createLongDataReadException(e, input.getClass(), paramIndex));
            }
        });
    }


    private Flux<ByteBuf> sendReader(final int paramIndex, final Reader input) {
        return Flux.create(sink -> {
            ByteBuf packetBuffer = null;
            try {
                final Charset clobCharset = obtainClobCharset();
                final CharBuffer charBuffer = CharBuffer.allocate(BUFFER_LENGTH >> 1);

                packetBuffer = createLongDataPacket(paramIndex, BUFFER_LENGTH);

                ByteBuffer byteBuffer;
                while (input.read(charBuffer) > 0) { //1. read char stream
                    byteBuffer = clobCharset.encode(charBuffer);   //2. encode char to byte
                    packetBuffer = writeByteBufferToBlobPacket(byteBuffer, paramIndex, packetBuffer, sink); // 3.write byte to packet.
                    charBuffer.clear(); // 4. clear charBuffer
                }
                if (hasBlobData(packetBuffer)) {
                    PacketUtils.writePacketHeader(packetBuffer, this.sequenceIdSupplier.get()); // write header
                    sink.next(packetBuffer); // send packet
                } else {
                    packetBuffer.release();
                }
            } catch (IOException e) {
                packetBuffer.release();
                sink.error(createLongDataReadException(e, input.getClass(), paramIndex));
            }
        });
    }


    private Flux<ByteBuf> sendPath(final int paramIndex, final Path path) {
        return Flux.create(sink -> {
            try (InputStream input = Files.newInputStream(path, StandardOpenOption.READ)) {
                writeInputStreamParameter(paramIndex, input, sink);
            } catch (IOException e) {
                sink.error(createLongDataReadException(e, path.getClass(), paramIndex));
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
        ByteBuf packetBuffer = this.adjutant.createPacketBuffer(payloadCapacity);

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

    private void writeInputStreamParameter(final int parameterIndex, InputStream input, FluxSink<ByteBuf> sink) {
        ByteBuf packetBuffer = null;
        try {
            packetBuffer = createLongDataPacket(parameterIndex, BUFFER_LENGTH << 1);
            final byte[] buffer = new byte[BUFFER_LENGTH];
            for (int length; (length = input.read(buffer)) > 0; ) {
                packetBuffer = writeByteArrayToBlobPacket(buffer, length, parameterIndex, packetBuffer, sink);
            }
            if (hasBlobData(packetBuffer)) {
                PacketUtils.writePacketHeader(packetBuffer, this.sequenceIdSupplier.get()); // write header
                sink.next(packetBuffer); // send packet
            } else {
                packetBuffer.release();
            }

        } catch (IOException e) {
            packetBuffer.release();
            sink.error(createLongDataReadException(e, input.getClass(), parameterIndex));
        }
    }

    private ByteBuf writeByteBufferToBlobPacket(final ByteBuffer byteBuffer, final int parameterIndex
            , ByteBuf packetBuffer, final FluxSink<ByteBuf> sink) {
        if (!byteBuffer.hasRemaining()) {
            return packetBuffer;
        }

        if (byteBuffer.remaining() <= packetBuffer.writableBytes()) {
            packetBuffer.writeBytes(byteBuffer);
            return packetBuffer;
        }

        for (int writableBytes; byteBuffer.hasRemaining(); ) {
            packetBuffer = addBlobPacketCapacity(packetBuffer, byteBuffer.remaining());
            writableBytes = packetBuffer.writableBytes();

            if (byteBuffer.remaining() > writableBytes) {
                if (writableBytes > 0) {
                    byte[] bufferArray = new byte[writableBytes];
                    byteBuffer.get(bufferArray);
                    packetBuffer.writeBytes(bufferArray);
                }
                PacketUtils.writePacketHeader(packetBuffer, this.sequenceIdSupplier.get()); // write header
                sink.next(packetBuffer); // send packet

                packetBuffer = createLongDataPacket(parameterIndex, byteBuffer.remaining()); // create new packet
            } else {
                packetBuffer.writeBytes(byteBuffer);
            }

        }
        return packetBuffer;

    }

    /**
     * @see #writeInputStreamParameter(int, InputStream, FluxSink)
     */
    private ByteBuf writeByteArrayToBlobPacket(final byte[] byteArray, final int length
            , final int parameterIndex, ByteBuf packetBuffer, final FluxSink<ByteBuf> sink) {
        if (length == 0) {
            return packetBuffer;
        }
        if (length <= packetBuffer.writableBytes()) {
            packetBuffer.writeBytes(byteArray, 0, length);
            return packetBuffer;
        }
        // below Adjusts the capacity of this buffer
        for (int offset = 0, writableBytes, dataLen; offset < length; ) {
            dataLen = length - offset;
            packetBuffer = addBlobPacketCapacity(packetBuffer, dataLen);
            writableBytes = packetBuffer.writableBytes();

            if (dataLen > writableBytes) {
                if (writableBytes > 0) {
                    packetBuffer.writeBytes(byteArray, offset, writableBytes);
                    offset += writableBytes;
                }
                PacketUtils.writePacketHeader(packetBuffer, this.sequenceIdSupplier.get()); // write header
                sink.next(packetBuffer); // send packet

                dataLen = length - offset;
                packetBuffer = createLongDataPacket(parameterIndex, dataLen); // create new packet
                dataLen = Math.min(dataLen, packetBuffer.writableBytes());
            }
            packetBuffer.writeBytes(byteArray, offset, dataLen);
            offset += dataLen;
        }
        return packetBuffer;
    }

    private ByteBuf addBlobPacketCapacity(final ByteBuf packetBuffer, final int addCapacity) {
        final int oldCapacity = packetBuffer.capacity();
        int require = Math.max(oldCapacity + addCapacity, oldCapacity << 1);
        if (require < 0) {
            require = this.maxPacketCapacity;
        }
        final int capacity = Math.min(this.maxPacketCapacity, require);
        ByteBuf buffer = packetBuffer;
        if (oldCapacity < capacity) {
            if (packetBuffer.maxCapacity() < capacity) {
                ByteBuf tempBuf = this.adjutant.createPayloadBuffer(capacity);
                tempBuf.writeBytes(packetBuffer);
                packetBuffer.release();
                buffer = tempBuf;
            } else {
                buffer = packetBuffer.capacity(capacity);
            }
        }
        return buffer;
    }

    private boolean hasBlobData(ByteBuf packetBuffer) {
        return packetBuffer.readableBytes() > (PacketUtils.HEADER_SIZE + LONG_DATA_PREFIX_SIZE);
    }

    private Charset obtainClobCharset() {
        Charset charset = this.properties.getProperty(PropertyKey.clobCharacterEncoding, Charset.class);
        if (charset == null) {
            charset = this.adjutant.obtainCharsetClient();
        }
        return charset;
    }

    /*################################## blow private instance inner class ##################################*/

    private final class PublisherLongDataSubscriber implements CoreSubscriber<Object> {

        private final FluxSink<ByteBuf> sink;

        private final int parameterIndex;

        ByteBuf packetBuffer;

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
            ByteBuf packetBuffer = this.packetBuffer;
            if (data instanceof byte[]) {
                byte[] byteArray = (byte[]) data;
                if (packetBuffer == null) {
                    packetBuffer = createLongDataPacket(this.parameterIndex, byteArray.length);
                }
                this.packetBuffer = writeByteArrayToBlobPacket(byteArray, byteArray.length
                        , this.parameterIndex, packetBuffer, this.sink);
            } else if (data instanceof ByteBuffer) {
                ByteBuffer byteBuffer = (ByteBuffer) data;
                if (packetBuffer == null) {
                    packetBuffer = createLongDataPacket(this.parameterIndex, byteBuffer.remaining());
                }
                this.packetBuffer = writeByteBufferToBlobPacket(byteBuffer, this.parameterIndex, packetBuffer, this.sink);
            } else {
                this.sink.error(new BindParameterException(
                        String.format("Bind parameter[%s] type[%s] error.", this.parameterIndex, data.getClass())
                        , this.parameterIndex));
            }
        }

        @Override
        public void onError(Throwable t) {
            ByteBuf packetBuffer = this.packetBuffer;
            if (packetBuffer != null) {
                packetBuffer.release();
            }
            this.sink.error(new BindParameterException(
                    String.format("Bind parameter[%s]'s publisher throw error", this.parameterIndex)
                    , t, this.parameterIndex));
        }

        @Override
        public void onComplete() {
            ByteBuf packetBuffer = this.packetBuffer;
            if (packetBuffer != null && hasBlobData(packetBuffer)) {
                PacketUtils.writePacketHeader(packetBuffer, sequenceIdSupplier.get()); // write header
                this.sink.next(packetBuffer); // send packet
                this.packetBuffer = null;
            }
        }


    }

    /*################################## blow private static method ##################################*/

    private static BindParameterException createLongDataReadException(IOException e, Class<?> parameterClass, int parameterIndex) {
        return new BindParameterException(
                String.format("Bind parameter[%s](%s) read error.", parameterIndex, parameterClass.getName())
                , e, parameterIndex);
    }


}
