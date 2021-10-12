package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;

import java.util.function.Supplier;

interface StmtTask {

    void addErrorToTask(Throwable error);

    boolean hasError();

    TaskAdjutant adjutant();

    /**
     * @return true: read CommandComplete message end , false : more cumulate.
     */
    boolean readResultStateWithReturning(ByteBuf cumulateBuffer, Supplier<Integer> resultIndexes);

    int getAndIncrementResultIndex();

    boolean isCanceled();

}
