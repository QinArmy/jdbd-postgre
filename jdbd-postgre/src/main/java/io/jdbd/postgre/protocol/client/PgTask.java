package io.jdbd.postgre.protocol.client;

import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.task.CommunicationTask;
import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;


abstract class PgTask extends CommunicationTask {

    final TaskAdjutant adjutant;

    final Properties properties;

    PostgreUnitTask unitTask;

    PgTask(final TaskAdjutant adjutant, Consumer<Throwable> errorConsumer) {
        super(adjutant, errorConsumer);
        this.adjutant = adjutant;
        this.properties = adjutant.obtainHost().getProperties();
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
