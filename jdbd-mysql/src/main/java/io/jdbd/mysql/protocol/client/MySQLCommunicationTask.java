package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.AbstractCommunicationTask;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

abstract class MySQLCommunicationTask extends AbstractCommunicationTask implements MySQLTask {

    static final int SEQUENCE_ID_MODEL = 256;


    final MySQLTaskAdjutant adjutant;

    final int negotiatedCapability;

    final Properties properties;

    private final AtomicInteger sequenceId = new AtomicInteger(-1);

    MySQLCommunicationTask(MySQLTaskAdjutant adjutant) {
        super(adjutant);
        this.negotiatedCapability = adjutant.obtainNegotiatedCapability();
        this.adjutant = adjutant;
        this.properties = adjutant.obtainHostInfo().getProperties();
    }

    public final int updateSequenceId(final int sequenceId) {
        final int newSequenceId;
        if (sequenceId < 0) {
            newSequenceId = -1;
        } else {
            newSequenceId = sequenceId % SEQUENCE_ID_MODEL;
        }
        this.sequenceId.set(newSequenceId);
        return newSequenceId;
    }

    public final int obtainSequenceId() {
        return this.sequenceId.get();
    }


    public final int addAndGetSequenceId() {
        return this.sequenceId.updateAndGet(operand -> (++operand) % SEQUENCE_ID_MODEL);
    }


    final int obtainMLen(final int collationIndex) {
        final int maxBytes;
        CharsetMapping.Collation collation = CharsetMapping.INDEX_TO_COLLATION.get(collationIndex);
        if (collation == null) {
            CharsetMapping.CustomCollation customCollation;
            customCollation = this.adjutant.obtainCustomCollationMap().get(collationIndex);
            if (customCollation == null) {
                throw new IllegalStateException(
                        String.format("collationIndex[%s] not found Collation.", collationIndex));
            }
            maxBytes = customCollation.maxLen;
        } else {
            maxBytes = collation.mySQLCharset.mblen;
        }
        return maxBytes;
    }

    final Charset obtainJavaCharset(final int collationIndex) {
        CharsetMapping.Collation collation = CharsetMapping.INDEX_TO_COLLATION.get(collationIndex);
        Charset charset;
        if (collation == null) {
            CharsetMapping.CustomCollation customCollation;
            customCollation = this.adjutant.obtainCustomCollationMap().get(collationIndex);
            if (customCollation == null) {
                throw new IllegalStateException(
                        String.format("collationIndex[%s] not found Collation.", collationIndex));
            }
            charset = CharsetMapping.getJavaCharsetByMySQLCharsetName(customCollation.charsetName);
            if (charset == null) {
                throw new IllegalStateException(
                        String.format("collationIndex[%s] not found java charset.", collationIndex));
            }
        } else {
            charset = Charset.forName(collation.mySQLCharset.javaEncodingsUcList.get(0));
        }
        return charset;
    }

    final Publisher<ByteBuf> createPacketPublisher(ByteBuf packetBuffer) {
        return Mono.empty();
    }

    final ByteBuf createResetConnectionPacket() {
        ByteBuf packet = this.adjutant.alloc().buffer(5);

        PacketUtils.writeInt3(packet, 1);
        packet.writeByte(addAndGetSequenceId());
        packet.writeByte(PacketUtils.COM_RESET_CONNECTION);
        return packet;
    }

    static ByteBuf commandBuffer(MySQLCommunicationTask task, String command) {
        int initialCapacity = task.adjutant.obtainMaxBytesPerCharClient() * command.length();
        ByteBuf byteBuf = task.adjutant.createPacketBuffer(initialCapacity);
        byte[] payload = command.getBytes(task.adjutant.obtainCharsetClient());

        if (payload.length > ClientProtocol.MAX_PACKET_SIZE) {
            byteBuf.writerIndex(byteBuf.readerIndex());
            writeBigBuffer(task, byteBuf, payload);
        } else {
            byteBuf.writeByte(PacketUtils.COM_QUERY)
                    .writeBytes(payload);
            PacketUtils.writePacketHeader(byteBuf, task.addAndGetSequenceId());
        }
        return byteBuf;
    }


    static void writeBigBuffer(MySQLCommunicationTask task, ByteBuf byteBuf, byte[] bigPayload) {
        for (int i = 0, len; i < bigPayload.length; i += len) {
            len = ClientProtocol.MAX_PACKET_SIZE;
            if (i + len > bigPayload.length) {
                len = bigPayload.length - i;
            }
            PacketUtils.writeInt3(byteBuf, len);
            byteBuf.writeByte(task.addAndGetSequenceId());
            if (i == 0) {
                byteBuf.writeByte(PacketUtils.COM_QUERY);
            }
            byteBuf.writeBytes(bigPayload, i, len);
        }
    }


    static JdbdSQLException createSequenceIdError(int expected, ByteBuf cumulateBuffer) {
        return MySQLExceptions.createFatalIoException(
                (Throwable) null
                , "MySQL server row packet return sequence_id error,expected[%s] actual[%s]"
                , expected, PacketUtils.getInt1(cumulateBuffer, cumulateBuffer.readerIndex() - 1));
    }

}
