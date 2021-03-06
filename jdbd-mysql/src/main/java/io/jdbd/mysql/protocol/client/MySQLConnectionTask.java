package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.task.AbstractCommunicationTask;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;


abstract class MySQLConnectionTask extends AbstractCommunicationTask implements MySQLTask {


    final MySQLTaskAdjutant adjutant;

    private int sequenceId = -1;


    MySQLConnectionTask(MySQLTaskAdjutant adjutant) {
        super(adjutant);
        this.adjutant = adjutant;
    }


    final int addAndGetSequenceId() {
        int sequenceId = this.sequenceId;
        sequenceId = (++sequenceId) & 0XFF;
        this.sequenceId = sequenceId;
        return sequenceId;
    }


    final int obtainSequenceId() {
        return this.sequenceId;
    }

    @Override
    public Publisher<ByteBuf> moreSendPacket() {
        return null;
    }


    final void updateSequenceId(int sequenceId) {
        this.sequenceId = sequenceId & 0XFF;
    }


}
