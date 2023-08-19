package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.DataType;
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

    List<? extends DataType> getParamTypes();

    @Nullable
    ResultRowMeta getRowMeta();

    void handleNoExecuteMessage();

}
