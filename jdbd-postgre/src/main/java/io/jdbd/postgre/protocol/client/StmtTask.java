package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;

interface StmtTask {

    void addResultSetError(Throwable error);

    TaskAdjutant adjutant();

    /**
     * @return true: read CommandComplete message end , false : more cumulate.
     */
    boolean readResultStateWithReturning(ByteBuf cumulateBuffer, boolean moreFetch);

    int getAndIncrementResultIndex();

}
