package io.jdbd.mysql;

import java.nio.charset.Charset;

public interface Server {

    boolean containSqlMode(SQLMode sqlMode);

    boolean isBackslashEscapes();

    Charset obtainCharsetClient();

    Charset obtainCharsetResults();

}
