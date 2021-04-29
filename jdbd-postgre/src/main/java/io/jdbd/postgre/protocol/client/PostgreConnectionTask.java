package io.jdbd.postgre.protocol.client;

import io.jdbd.config.PropertyException;
import io.jdbd.postgre.ServerVersion;
import io.jdbd.postgre.config.PGKey;
import io.jdbd.postgre.config.PostgreHost;
import io.jdbd.postgre.util.PostgreExceptions;
import io.jdbd.postgre.util.PostgreTimes;
import io.jdbd.vendor.task.ConnectionTask;
import io.jdbd.vendor.task.SslWrapper;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;


/**
 * @see <a href="https://www.postgresql.org/docs/11/protocol-flow.html#id-1.10.5.7.3">Protocol::Start-up</a>
 * @see <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">Message::StartupMessage (F)</a>
 */
final class PostgreConnectionTask extends PostgreTask implements ConnectionTask {

    static Mono<AuthResult> authenticate(TaskAdjutant adjutant) {
        return Mono.create(sink -> {

            try {
                PostgreConnectionTask task = new PostgreConnectionTask(sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PostgreExceptions.wrap(e));
            }

        });
    }

    private static final Logger LOG = LoggerFactory.getLogger(PostgreConnectionTask.class);

    private final MonoSink<AuthResult> sink;

    private Phase phase;

    private PostgreConnectionTask(MonoSink<AuthResult> sink, TaskAdjutant adjutant) {
        super(adjutant);
        this.sink = sink;
    }

    /*################################## blow ConnectionTask method ##################################*/

    @Override
    public final void addSsl(Consumer<SslWrapper> sslConsumer) {
        //no-op
    }

    @Override
    public final boolean disconnect() {
        return this.phase == Phase.DISCONNECT;
    }

    /*################################## blow protected template method ##################################*/

    @Override
    protected final Publisher<ByteBuf> start() {

        ByteBuf packet = null;
        try {
            packet = createStartUpPacket();
        } catch (Throwable e) {
            handleError(e);
        }
        final Publisher<ByteBuf> publisher;
        if (packet == null) {
            publisher = null;
        } else {
            publisher = Mono.just(packet);
        }
        return publisher;
    }

    @Override
    protected final Action onError(Throwable e) {
        return Action.TASK_END;
    }


    @Override
    protected final boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        boolean taskEnd = false, continueDecode = true;
        while (continueDecode) {
            switch (this.phase) {
                case READ_START_UP_RESPONSE: {
                    taskEnd = readStartUpResponse(cumulateBuffer, serverStatusConsumer);

                }
                break;
                default:

            }

        }

        return taskEnd;
    }

    /*################################## blow private method ##################################*/

    private boolean readStartUpResponse(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        return false;
    }


    /**
     * @throws PropertyException property can't convert
     * @see <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">Protocol::StartupMessage</a>
     * @see <a href="https://www.postgresql.org/docs/11/protocol-overview.html#PROTOCOL-MESSAGE-CONCEPTS">Messaging Overview</a>
     */
    private ByteBuf createStartUpPacket() {
        final ByteBuf packet = this.adjutant.allocator().buffer(1024);
        // For historical reasons, the very first message sent by the client (the startup message) has no initial message-type byte.
        packet.writeZero(4); // length placeholder

        packet.writeShort(3); // protocol major
        packet.writeShort(0); // protocol major
        for (String[] paramPair : obtainStartUpParamList()) {
            Packets.writeString(packet, paramPair[0]);
            Packets.writeString(packet, paramPair[1]);
        }
        packet.writeByte(0);// Terminating \0

        final int readableBytes = packet.readableBytes(), writerIndex = packet.writerIndex();
        packet.writerIndex(packet.readerIndex());
        packet.writeInt(readableBytes);
        packet.writerIndex(writerIndex);
        return packet;
    }


    /**
     * @see #createStartUpPacket()
     */
    private List<String[]> obtainStartUpParamList() {
        final PostgreHost host = this.adjutant.obtainHost();
        final ServerVersion minVersion;
        minVersion = this.properties.getProperty(PGKey.assumeMinServerVersion, ServerVersion.class
                , ServerVersion.INVALID);

        List<String[]> list = new ArrayList<>();

        list.add(new String[]{"user", host.getUser()});
        list.add(new String[]{"database", host.getNonNullDbName()});
        list.add(new String[]{"client_encoding", Packets.CLIENT_CHARSET.name()});
        list.add(new String[]{"DateStyle", "ISO"});

        list.add(new String[]{"TimeZone", PostgreTimes.systemZoneOffset().normalized().getId()});
        list.add(new String[]{"jdbd", getDriverName()});

        if (minVersion.compareTo(ServerVersion.V9_0) >= 0) {
            list.add(new String[]{"extra_float_digits", "3"});
            list.add(new String[]{"application_name", this.properties.getOrDefault(PGKey.ApplicationName)});
        } else {
            list.add(new String[]{"extra_float_digits", "2"});
        }

        if (minVersion.compareTo(ServerVersion.V9_4) >= 0) {
            String replication = this.properties.getProperty(PGKey.replication);
            if (replication != null) {
                list.add(new String[]{"replication", replication});
            }
        }

        final String currentSchema = this.properties.getProperty(PGKey.currentSchema);
        if (currentSchema != null) {
            list.add(new String[]{"search_path", currentSchema});
        }
        final String options = this.properties.getProperty(PGKey.options);
        if (options != null) {
            list.add(new String[]{"search_path", options});
        }

        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            int count = 0;
            final String separator = System.lineSeparator();
            for (String[] pair : list) {
                if (count > 0) {
                    builder.append(",");
                }
                builder.append(separator)
                        .append(pair[0])
                        .append("=")
                        .append(pair[1]);
                count++;
            }
            LOG.debug("StartupPacket[{}]", builder);
        }
        return list;
    }

    /**
     * @see #start()
     */
    private void handleError(Throwable e) {
        this.phase = Phase.DISCONNECT;
        this.sink.error(PostgreExceptions.wrap(e));

    }

    /**
     * @see #obtainStartUpParamList()
     */
    private String getDriverName() {
        String driverName = ClientProtocol.class.getPackage().getImplementationVersion();
        if (driverName == null) {
            driverName = "jdbd-postgre-test";
        }
        return driverName;
    }


    private enum Phase {
        READ_START_UP_RESPONSE,
        DISCONNECT
    }

}
