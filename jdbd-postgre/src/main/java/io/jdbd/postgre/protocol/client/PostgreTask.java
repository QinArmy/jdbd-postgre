package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.postgre.config.PGKey;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.task.CommunicationTask;
import io.netty.buffer.ByteBuf;

import java.util.List;


abstract class PostgreTask extends CommunicationTask<TaskAdjutant> {


    final Properties<PGKey> properties;

    List<JdbdException> errorList;

    PostgreUnitTask unitTask;

    PostgreTask(final TaskAdjutant adjutant) {
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
