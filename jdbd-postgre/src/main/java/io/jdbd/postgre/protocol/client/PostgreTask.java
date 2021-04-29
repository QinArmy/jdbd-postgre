package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.config.PGKey;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.task.CommunicationTask;
import io.netty.buffer.ByteBuf;


abstract class PostgreTask extends CommunicationTask {

    final TaskAdjutant adjutant;

    final Properties<PGKey> properties;

    PostgreTask(final TaskAdjutant adjutant) {
        super(adjutant);
        this.adjutant = adjutant;
        this.properties = adjutant.obtainHost().getProperties();
    }


    @Override
    protected final boolean hasOnePacket(ByteBuf cumulateBuffer) {
        return Packets.hasOnePacket(cumulateBuffer);
    }


}
