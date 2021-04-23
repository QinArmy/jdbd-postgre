package io.jdbd.mysql;

import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.time.ZoneOffset;

public interface Server {

    boolean containSqlMode(SQLMode sqlMode);

    Charset obtainCharsetClient();

    @Nullable
    Charset obtainCharsetResults();

    ZoneOffset obtainZoneOffsetDatabase();

    ZoneOffset obtainZoneOffsetClient();

    boolean supportLocalInfile();

}
