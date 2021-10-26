package io.jdbd.mysql.protocol.client;

import io.jdbd.result.Result;

interface StmtTask extends MetaAdjutant {

 boolean isCancelled();

 void next(Result result);

 void addErrorToTask(Throwable error);

 TaskAdjutant adjutant();

 void updateSequenceId(int sequenceId);


    int nextResultIndex();


}
