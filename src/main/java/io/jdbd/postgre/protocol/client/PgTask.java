package io.jdbd.postgre.protocol.client;

import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.task.CommunicationTask;
import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;


abstract class PgTask extends CommunicationTask {

    final TaskAdjutant adjutant;

    final Environment env;

    PostgreUnitTask unitTask;

    PgTask(final TaskAdjutant adjutant, Consumer<Throwable> errorConsumer) {
        super(adjutant, errorConsumer);
        this.adjutant = adjutant;
        this.env = adjutant.environment();
    }

    /**
     * <p>
     * If use this constructor ,then must override {@link #emitError(Throwable)}
     * </p>
     */
    PgTask(final TaskAdjutant adjutant) {
        super(adjutant);
        this.adjutant = adjutant;
        this.env = adjutant.environment();
    }


    /**
     * <p>
     * must invoke after ReadyForQuery message.
     * </p>
     */
    final void readNoticeAfterReadyForQuery(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        while (Messages.hasOneMessage(cumulateBuffer)) {
            if (cumulateBuffer.getByte(cumulateBuffer.readerIndex()) != Messages.N) {// NoticeResponse message
                break;
            }
            NoticeMessage noticeMessage = NoticeMessage.read(cumulateBuffer, this.adjutant.clientCharset());
            serverStatusConsumer.accept(noticeMessage);
        }

    }


}
