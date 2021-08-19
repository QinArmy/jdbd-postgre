package io.jdbd.postgre;

import io.jdbd.postgre.protocol.client.IntervalStyle;

import java.time.ZoneOffset;

public interface Server {

    ServerVersion serverVersion();

    String parameter(ServerParameter parameter);

    ZoneOffset zoneOffset();

    IntervalStyle intervalStyle();


}
