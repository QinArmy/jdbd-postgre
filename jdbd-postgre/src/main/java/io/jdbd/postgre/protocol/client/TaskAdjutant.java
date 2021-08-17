package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.config.PostgreHost;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.vendor.task.ITaskAdjutant;

import java.nio.charset.Charset;
import java.time.ZoneOffset;

interface TaskAdjutant extends ITaskAdjutant {


    PostgreHost obtainHost();

    long processId();

    int serverSecretKey();

    Charset clientCharset();

    @Deprecated
    ZoneOffset clientOffset();

    PgParser sqlParser();

    TxStatus txStatus();


}
