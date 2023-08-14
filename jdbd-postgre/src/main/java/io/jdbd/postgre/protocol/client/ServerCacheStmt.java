package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.result.ResultRowMeta;
import reactor.util.annotation.Nullable;

import java.util.List;

interface ServerCacheStmt extends CacheStmt {

    String stmtName();

    List<DataType> getParamOidList();

    @Nullable
    ResultRowMeta getRowMeta();


}
