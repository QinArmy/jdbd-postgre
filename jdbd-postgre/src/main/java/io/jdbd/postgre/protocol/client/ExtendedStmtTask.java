package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import reactor.util.annotation.Nullable;

import java.util.List;

interface ExtendedStmtTask {

    TaskAdjutant adjutant();

    @Nullable
    String getNewPortalName();

    @Nullable
    String getStatementName();

    List<PgType> getParamTypeList();

    int getFetchSize();

}
