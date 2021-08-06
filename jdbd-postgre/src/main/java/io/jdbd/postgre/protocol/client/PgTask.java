package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.config.PgKey;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.task.CommunicationTask;
import io.netty.buffer.ByteBuf;


abstract class PgTask extends CommunicationTask<TaskAdjutant> {


    final Properties<PgKey> properties;

    PostgreUnitTask unitTask;

    PgTask(final TaskAdjutant adjutant) {
        super(adjutant);
        this.properties = adjutant.obtainHost().getProperties();
    }


    @Override
    protected final boolean hasOnePacket(ByteBuf cumulateBuffer) {
        final PostgreUnitTask unitTask = this.unitTask;
        final boolean yes;
        if (unitTask == null) {
            yes = Messages.hasOneMessage(cumulateBuffer);
        } else {
            yes = unitTask.hasOnePacket(cumulateBuffer);
        }
        return yes;
    }


}
