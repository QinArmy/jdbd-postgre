package io.jdbd.mysql.protocol.client;

import io.jdbd.result.Result;
import io.netty.buffer.ByteBuf;

import java.util.function.Supplier;

interface StmtTask extends MetaAdjutant {

 boolean isCancelled();

 void next(Result result);

 void addErrorToTask(Throwable error);

 TaskAdjutant adjutant();

 void updateSequenceId(int sequenceId);

 /**
  * @return true: read CommandComplete message end , false : more cumulate.
  */
    boolean readResultStateWithReturning(ByteBuf cumulateBuffer, Supplier<Integer> resultIndexes);

    int getAndIncrementResultIndex();


}
