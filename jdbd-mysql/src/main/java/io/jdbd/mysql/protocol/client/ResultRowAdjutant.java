package io.jdbd.mysql.protocol.client;

import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.time.ZoneOffset;

interface ResultRowAdjutant {

    ZoneOffset obtainZoneOffsetClient();

    @Nullable
    Charset getCharsetResults();


}
