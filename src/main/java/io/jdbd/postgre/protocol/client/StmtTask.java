package io.jdbd.postgre.protocol.client;

import io.jdbd.result.ResultItem;
import io.netty.buffer.ByteBuf;

import java.util.function.IntSupplier;

interface StmtTask {

    boolean isCancelled();

    void next(ResultItem result);

    void addErrorToTask(Throwable error);

    boolean hasError();

    TaskAdjutant adjutant();

    /**
     * @return true: read CommandComplete message end , false : more cumulate.
     */
    boolean readResultStateOfQuery(ByteBuf cumulateBuffer, IntSupplier resultIndexes);

    int nextResultNo();


}
