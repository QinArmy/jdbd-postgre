package io.jdbd.mysql.protocol.client;

import io.jdbd.result.Result;

import java.nio.file.Path;

interface StmtTask extends MetaAdjutant {

 boolean isCancelled();

 void next(Result result);

 void addErrorToTask(Throwable error);

 void addBigColumnPath(Path path);

 TaskAdjutant adjutant();

 void updateSequenceId(int sequenceId);


 int nextResultIndex();


}
