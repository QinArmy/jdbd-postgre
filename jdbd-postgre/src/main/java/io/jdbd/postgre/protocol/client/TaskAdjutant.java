package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.config.PostgreHost;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.vendor.task.ITaskAdjutant;

import java.nio.charset.Charset;
import java.time.ZoneOffset;

interface TaskAdjutant extends ITaskAdjutant {


    PostgreHost obtainHost();

    long processId();

    Charset clientCharset();

    ZoneOffset clientOffset();

    PgParser sqlParser();

}
