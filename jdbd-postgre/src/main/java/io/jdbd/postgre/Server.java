package io.jdbd.postgre;

import java.time.ZoneOffset;

public interface Server {

    ServerVersion serverVersion();

    String parameter(ServerParameter parameter);

    ZoneOffset zoneOffset();

}
