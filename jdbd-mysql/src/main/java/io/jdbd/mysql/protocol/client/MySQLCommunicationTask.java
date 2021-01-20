package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.vendor.AbstractCommunicationTask;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;

abstract class MySQLCommunicationTask extends AbstractCommunicationTask implements MySQLTask {

    static final int SEQUENCE_ID_MODEL = 256;

    final MySQLTaskAdjutant executorAdjutant;

    final int negotiatedCapability;

    final Properties properties;

    private int sequenceId = -1;

    MySQLCommunicationTask(MySQLTaskAdjutant executorAdjutant) {
        super(executorAdjutant);
        this.negotiatedCapability = executorAdjutant.obtainNegotiatedCapability();
        this.executorAdjutant = executorAdjutant;
        this.properties = executorAdjutant.obtainHostInfo().getProperties();
    }

    final void updateSequenceId(int sequenceId) {
        this.sequenceId = sequenceId % SEQUENCE_ID_MODEL;
    }

    final int obtainSequenceId() {
        return this.sequenceId;
    }


    final int addAndGetSequenceId() {
        int sequenceId = this.sequenceId;
        sequenceId = (++sequenceId) % SEQUENCE_ID_MODEL;
        this.sequenceId = sequenceId;
        return sequenceId;
    }

    final int obtainMLen(final int collationIndex) {
        final int maxBytes;
        CharsetMapping.Collation collation = CharsetMapping.INDEX_TO_COLLATION.get(collationIndex);
        if (collation == null) {
            CharsetMapping.CustomCollation customCollation;
            customCollation = this.executorAdjutant.obtainCustomCollationMap().get(collationIndex);
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
            customCollation = this.executorAdjutant.obtainCustomCollationMap().get(collationIndex);
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


    static ByteBuf commandBuffer(MySQLCommunicationTask task, String command) {
        int initialCapacity = task.executorAdjutant.obtainMaxBytesPerCharClient() * command.length();
        ByteBuf byteBuf = task.executorAdjutant.createPacketBuffer(initialCapacity);
        byte[] payload = command.getBytes(task.executorAdjutant.obtainCharsetClient());

        if (payload.length > ClientProtocol.MAX_PACKET_SIZE) {
            byteBuf.writerIndex(byteBuf.readerIndex());
            writeBigBuffer(task, byteBuf, payload);
        } else {
            byteBuf.writeByte(PacketUtils.COM_QUERY_HEADER)
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
                byteBuf.writeByte(PacketUtils.COM_QUERY_HEADER);
            }
            byteBuf.writeBytes(bigPayload, i, len);
        }
    }


    static JdbdSQLException createSequenceIdError(int expected, ByteBuf cumulateBuffer) {
        return MySQLExceptionUtils.createFatalIoException(
                (Throwable) null
                , "MySQL server row packet return sequence_id error,expected[%s] actual[%s]"
                , expected, PacketUtils.getInt1(cumulateBuffer, cumulateBuffer.readerIndex() - 1));
    }

}
