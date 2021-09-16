package io.jdbd.postgre;

import io.jdbd.postgre.protocol.client.IntervalStyle;

import java.time.ZoneOffset;
import java.util.Locale;

public interface Server {

    PgServerVersion serverVersion();

    String parameter(ServerParameter parameter);

    ZoneOffset zoneOffset();

    IntervalStyle intervalStyle();

    Locale moneyLocal();


}
