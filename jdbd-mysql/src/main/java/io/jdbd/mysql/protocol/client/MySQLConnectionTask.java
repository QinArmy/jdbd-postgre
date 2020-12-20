package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.AbstractCommunicationTask;
import io.netty.buffer.ByteBuf;

import java.nio.file.Path;


abstract class MySQLConnectionTask extends AbstractCommunicationTask implements MySQLTask {


    final MySQLTaskAdjutant executorAdjutant;

    final int negotiatedCapability;

    private int sequenceId;


    MySQLConnectionTask(MySQLTaskAdjutant executorAdjutant, int sequenceId) {
        super(executorAdjutant);
        this.executorAdjutant = executorAdjutant;
        this.negotiatedCapability = (this instanceof HandshakeV10Task) ? 0 : executorAdjutant.obtainNegotiatedCapability();
        this.sequenceId = sequenceId;
    }


    @Override
    public int addAndGetSequenceId() {
        int sequenceId = this.sequenceId;
        sequenceId = (++sequenceId) % 256;
        this.sequenceId = sequenceId;
        return sequenceId;
    }



    final int getSequenceId() {
        return this.sequenceId;
    }

    @Override
    public ByteBuf moreSendPacket() {
        return null;
    }

    @Override
    public Path moreSendFile() {
        return null;
    }

    final void updateSequenceId(int sequenceId) {
        this.sequenceId = sequenceId % 256;
    }


}
