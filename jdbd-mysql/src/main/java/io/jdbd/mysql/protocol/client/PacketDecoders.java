package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.nio.charset.Charset;
import java.util.function.LongConsumer;

public abstract class PacketDecoders {


    protected PacketDecoders() {
        throw new UnsupportedOperationException();
    }

    private static final Logger LOG = LoggerFactory.getLogger(PacketDecoders.class);


    public static boolean packetDecoder(final ByteBuf cumulateBuffer, MonoSink<ByteBuf> sink) {
        final int readableBytes = cumulateBuffer.readableBytes();
        boolean decodeEnd;
        if (readableBytes < PacketUtils.HEADER_SIZE) {
            decodeEnd = false;
        } else {
            final int packetLength;
            packetLength = PacketUtils.HEADER_SIZE + PacketUtils.getInt3(cumulateBuffer, cumulateBuffer.readerIndex());
            if (readableBytes < packetLength) {
                decodeEnd = false;
            } else {
                decodeEnd = true;

                sink.success(cumulateBuffer.readRetainedSlice(packetLength));
            }
        }

        return decodeEnd;
    }

    public static ErrorPacket decodeErrorPacket(final ByteBuf cumulateBuffer, final int negotiatedCapability
            , Charset charset) {
        ByteBuf packetBuf = cumulateBuffer.readSlice(PacketUtils.HEADER_SIZE
                + PacketUtils.getInt3(cumulateBuffer, cumulateBuffer.readerIndex()));

        if (PacketUtils.getInt1(packetBuf, packetBuf.readerIndex()) != ErrorPacket.ERROR_HEADER) {
            throw new IllegalArgumentException("cumulateBuffer error ,no error packet.");
        }
        packetBuf.skipBytes(PacketUtils.HEADER_SIZE);
        return ErrorPacket.readPacket(packetBuf, negotiatedCapability, charset);
    }

    public static Mono<Void> checkError(final ByteBuf packetBuf, final int negotiatedCapability
            , Charset charset) {
        int type = PacketUtils.getInt1(packetBuf, packetBuf.readerIndex() + PacketUtils.HEADER_SIZE);
        if (type == ErrorPacket.ERROR_HEADER) {
            packetBuf.skipBytes(PacketUtils.HEADER_SIZE);
            ErrorPacket error = ErrorPacket.readPacket(packetBuf, negotiatedCapability, charset);
            return Mono.error(MySQLExceptionUtils.createErrorPacketException(error));
        }
        return Mono.empty();

    }



    /*################################## blow package method ##################################*/









    /*################################## blow private method ##################################*/

    /**
     * use when drop packet
     */
    static final MonoSink<ByteBuf> SEWAGE_MONO_SINK = new MonoSink<ByteBuf>() {
        @Override
        public Context currentContext() {
            return Context.empty();
        }

        @Override
        public void success() {
            //no-op
        }

        @Override
        public void success(@Nullable ByteBuf value) {
            //no-op
        }

        @Override
        public void error(Throwable e) {
            //no-op
        }

        @Override
        public MonoSink<ByteBuf> onRequest(LongConsumer consumer) {
            return this;
        }

        @Override
        public MonoSink<ByteBuf> onCancel(Disposable d) {
            return this;
        }

        @Override
        public MonoSink<ByteBuf> onDispose(Disposable d) {
            return this;
        }
    };

    static final FluxSink<ByteBuf> SEWAGE_FLUX_SINK = new FluxSink<ByteBuf>() {
        @Override
        public void complete() {
            //no-op
        }

        @Override
        public Context currentContext() {
            //no-op
            return Context.empty();
        }

        @Override
        public void error(Throwable e) {
            //no-op
        }

        @Override
        public FluxSink<ByteBuf> next(ByteBuf byteBuf) {
            //no-op
            return this;
        }

        @Override
        public long requestedFromDownstream() {
            //no-op
            return 0;
        }

        @Override
        public boolean isCancelled() {
            //no-op
            return false;
        }

        @Override
        public FluxSink<ByteBuf> onRequest(LongConsumer consumer) {
            //no-op
            return this;
        }

        @Override
        public FluxSink<ByteBuf> onCancel(Disposable d) {
            //no-op
            return this;
        }

        @Override
        public FluxSink<ByteBuf> onDispose(Disposable d) {
            //no-op
            return this;
        }
    };


}
