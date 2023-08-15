package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.task.ITaskAdjutant;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.function.IntFunction;

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

    ProtocolFactory factory();

    PostgreStmt parseAsPostgreStmt(String sql);

    String nextStmtName();

    String nextPortName(String stmtName);


    IntFunction<DataType> oidToDataTypeFunc();

    void cachePostgreStmt(String sql, List<DataType> paramTypeList, PgRowMeta rowMeta);

    void appendSetCommandParameter(String parameterName);

    DataType internalOrUserType(String upperCaseName);

    boolean isNeedQueryUnknownType(Set<String> unknownTypeSet);

    Mono<Void> queryUnknownTypesIfNeed(Set<String> unknownTypeSet);


}
