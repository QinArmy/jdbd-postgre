package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;

import java.util.function.Supplier;

interface StmtTask {

    void addResultSetError(Throwable error);

    boolean hasError();

    TaskAdjutant adjutant();

    /**
     * @return true: read CommandComplete message end , false : more cumulate.
     */
    boolean readResultStateWithReturning(ByteBuf cumulateBuffer, boolean moreFetch, Supplier<Integer> resultIndexes);

    int getAndIncrementResultIndex();

}
