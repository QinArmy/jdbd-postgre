package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.vendor.stmt.ParamSingleStmt;
import reactor.util.annotation.Nullable;

import java.util.List;

/**
 * @see ExtendedCommandWriter
 */
interface ExtendedStmtTask {

    void addErrorToTask(Throwable error);

    ParamSingleStmt getStmt();

    TaskAdjutant adjutant();

    List<PgType> getParamTypes();

    @Nullable
    ResultRowMeta getRowMeta();

    void handleNoExecuteMessage();

}
