package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Server;
import io.jdbd.postgre.config.PostgreHost;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.vendor.task.ITaskAdjutant;

import java.nio.charset.Charset;
import java.time.ZoneOffset;

interface TaskAdjutant extends ITaskAdjutant {

    /**
     * @return always same instance
     */
    PostgreHost obtainHost();

    /**
     * @return maybe different instance
     */
    long processId();

    /**
     * @return maybe different instance
     */
    int serverSecretKey();

    /**
     * @return maybe different instance
     */
    Charset clientCharset();

    String createPrepareName();

    String createPortalName();

    @Deprecated
    ZoneOffset clientOffset();

    /**
     * @return maybe different instance
     */
    PgParser sqlParser();

    /**
     * @return maybe different instance
     */
    TxStatus txStatus();

    /**
     * @return maybe different instance
     */
    Server server();


    void appendSetCommandParameter(String parameterName);


}
