package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgReConnectableException;
import io.jdbd.postgre.env.Enums;
import io.jdbd.vendor.task.SslWrapper;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

final class SslUnitTask extends PostgreUnitTask {

    static SslUnitTask ssl(PgConnectionTask task, Consumer<SslWrapper> sslWrapperConsumer) {
        return new SslUnitTask(task, sslWrapperConsumer);
    }

    private static final Logger LOG = LoggerFactory.getLogger(SslUnitTask.class);

    private final Consumer<SslWrapper> sslWrapperConsumer;

    private Phase phase;

    private SslUnitTask(PgConnectionTask task, Consumer<SslWrapper> sslWrapperConsumer) {
        super(task);
        this.sslWrapperConsumer = sslWrapperConsumer;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Protocol::SSLRequest</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-overview.html#PROTOCOL-MESSAGE-CONCEPTS">Messaging Overview</a>
     */
    @Override
    public final Publisher<ByteBuf> start() {
        final Publisher<ByteBuf> publisher;
        if (this.phase == null) {
            ByteBuf message = this.adjutant.allocator().buffer(8);
            // For historical reasons, the very first message sent by the client (the startup message) has no initial message-type byte.
            message.writeInt(8);
            message.writeShort(1234);
            message.writeShort(5680);
            publisher = Mono.just(message);
            this.phase = Phase.READ_SSL_RESPONSE;
        } else {
            publisher = null;
        }
        return publisher;
    }

    @Override
    public final boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        final int singleByte = cumulateBuffer.readChar();
        final boolean reConnect = !this.properties.getOrDefault(PgKey0.sslmode, Enums.SslMode.class).needSslEnc();
        switch (singleByte) {
            case Messages.E: {
                String message = "Postgre server response error,not support SSL encryption.";
                LOG.debug(message);
                addException(new PgReConnectableException(reConnect, message));
            }
            break;
            case Messages.N: {
                if (!reConnect) {
                    addException(new PgReConnectableException(false, "Postgre server not support ssl."));
                }
            }
            break;
            case Messages.S: {

            }
            break;
            default:
        }
        this.phase = Phase.END;
        return true;
    }

    @Override
    public final boolean hasOnePacket(ByteBuf cumulateBuffer) {
        // a single byte response.
        return cumulateBuffer.isReadable();
    }


    private enum Phase {
        READ_SSL_RESPONSE,
        END
    }


}
