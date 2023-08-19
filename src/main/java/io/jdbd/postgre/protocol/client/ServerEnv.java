package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgServerVersion;
import io.jdbd.postgre.ServerParameter;

import java.time.ZoneOffset;
import java.util.Locale;

interface ServerEnv {

    PgServerVersion serverVersion();

    String parameter(ServerParameter parameter);

    ZoneOffset serverZone();

    IntervalStyle intervalStyle();

    DateStyle dateStyle();

    Locale moneyLocal();

}
