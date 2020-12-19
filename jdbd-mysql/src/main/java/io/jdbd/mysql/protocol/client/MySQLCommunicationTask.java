package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.vendor.AbstractCommunicationTask;
import io.netty.buffer.ByteBuf;

abstract class MySQLCommunicationTask extends AbstractCommunicationTask implements MySQLTask {

    static final int SEQUENCE_ID_MODEL = 256;

    final MySQLTaskAdjutant executorAdjutant;

    final int negotiatedCapability;

    private int sequenceId = -1;

    MySQLCommunicationTask(MySQLTaskAdjutant executorAdjutant) {
        super(executorAdjutant);
        this.negotiatedCapability = executorAdjutant.obtainNegotiatedCapability();
        this.executorAdjutant = executorAdjutant;
    }

    final void updateSequenceId(int sequenceId) {
        this.sequenceId = sequenceId % SEQUENCE_ID_MODEL;
    }

    final int obtainSequenceId() {
        return this.sequenceId;
    }

    @Override
    public final int addAndGetSequenceId() {
        int sequenceId = this.sequenceId;
        sequenceId = (++sequenceId) % SEQUENCE_ID_MODEL;
        this.sequenceId = sequenceId;
        return sequenceId;
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
