package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.config.PgKey;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.task.CommunicationTask;
import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;


abstract class PgTask extends CommunicationTask<TaskAdjutant> {


    final Properties<PgKey> properties;

    PostgreUnitTask unitTask;

    PgTask(final TaskAdjutant adjutant) {
        super(adjutant);
        this.properties = adjutant.obtainHost().getProperties();
    }


    @Override
    protected boolean canDecode(ByteBuf cumulateBuffer) {
        final PostgreUnitTask unitTask = this.unitTask;
        final boolean yes;
        if (unitTask == null) {
            yes = Messages.hasOneMessage(cumulateBuffer);
        } else {
            yes = unitTask.hasOnePacket(cumulateBuffer);
        }
        return yes;
    }


    /**
     * <p>
     * must invoke after ReadyForQuery message.
     * </p>
     */
    final void readNoticeAfterReadyForQuery(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        while (Messages.hasOneMessage(cumulateBuffer)) {
            if (cumulateBuffer.getByte(cumulateBuffer.readerIndex()) == Messages.N) {// NoticeResponse message
                NoticeMessage noticeMessage = NoticeMessage.read(cumulateBuffer, this.adjutant.clientCharset());
                serverStatusConsumer.accept(noticeMessage);
            } else {
                break;
            }
        }

    }


}
