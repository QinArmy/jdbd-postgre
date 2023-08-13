package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.task.ITaskAdjutant;

import java.nio.charset.Charset;
import java.time.ZoneOffset;

interface TaskAdjutant extends ITaskAdjutant, PgParser {

    /**
     * @return always same instance
     */
    PgHost0 obtainHost();

    Environment environment();

    /**
     * @return maybe different instance
     */
    long processId();

    boolean inTransaction();

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


    DataType handleUnknownType(int typeOid);

    /**
     * @return maybe different instance
     */
    TxStatus txStatus();

    /**
     * @return maybe different instance
     */
    ServerEnv server();


    void appendSetCommandParameter(String parameterName);


}
