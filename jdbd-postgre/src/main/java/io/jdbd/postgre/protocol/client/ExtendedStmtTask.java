package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.vendor.stmt.ParamSingleStmt;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.util.List;

/**
 * @see ExtendedCommandWriter
 */
interface ExtendedStmtTask {

    void addErrorToTask(Throwable error);

    boolean hasError();

    ParamSingleStmt getStmt();

    TaskAdjutant adjutant();

    @Nullable
    String getNewPortalName();

    @Nullable
    String getStatementName();

    List<PgType> getParamTypeList();

    int getFetchSize();

    void appendDescribePortalMessage(List<ByteBuf> messageList);

    void handleNoExecuteMessage();

}
