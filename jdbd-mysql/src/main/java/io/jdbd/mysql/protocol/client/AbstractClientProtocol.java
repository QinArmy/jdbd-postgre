package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractClientProtocol implements ClientProtocol, ClientProtocolAdjutant {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractClientProtocol.class);

    final HostInfo hostInfo;

    final MySQLTaskAdjutant taskAdjutant;

    final Properties properties;

    AbstractClientProtocol(HostInfo hostInfo, MySQLTaskAdjutant taskAdjutant) {
        this.hostInfo = hostInfo;
        this.taskAdjutant = taskAdjutant;
        this.properties = this.hostInfo.getProperties();
    }


    @Override
    public final HostInfo obtainHostInfo() {
        return this.hostInfo;
    }


}
