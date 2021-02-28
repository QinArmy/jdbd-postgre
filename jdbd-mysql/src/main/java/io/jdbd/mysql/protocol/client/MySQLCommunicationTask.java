package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.AbstractCommunicationTask;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

abstract class MySQLCommunicationTask extends AbstractCommunicationTask implements MySQLTask {


    private static final AtomicIntegerFieldUpdater<MySQLCommunicationTask> SEQUENCE_ID =
            AtomicIntegerFieldUpdater.newUpdater(MySQLCommunicationTask.class, "sequenceId");


    final MySQLTaskAdjutant adjutant;

    final int negotiatedCapability;

    final Properties properties;

    private volatile int sequenceId = -1;

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
            newSequenceId = sequenceId & 0XFF;
        }
        SEQUENCE_ID.set(this, newSequenceId);
        return newSequenceId;
    }

    public final int obtainSequenceId() {
        return SEQUENCE_ID.get(this);
    }


    public final int addAndGetSequenceId() {
        return SEQUENCE_ID.updateAndGet(this, operand -> (++operand) & 0XFF);
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
