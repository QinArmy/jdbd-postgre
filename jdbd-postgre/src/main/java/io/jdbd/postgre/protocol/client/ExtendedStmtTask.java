package io.jdbd.postgre.protocol.client;

import reactor.util.annotation.Nullable;

interface ExtendedStmtTask {

    TaskAdjutant adjutant();

    @Nullable
    String getNewPortalName();

    @Nullable
    String getStatementName();

    int getFetchSize();

}
