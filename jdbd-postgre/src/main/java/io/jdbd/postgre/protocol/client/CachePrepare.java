package io.jdbd.postgre.protocol.client;

import io.jdbd.result.ResultRowMeta;
import reactor.util.annotation.Nullable;

import java.util.List;

interface CachePrepare {

    String getSql();

    String getReplacedSql();

    List<Integer> getParamOidList();

    @Nullable
    ResultRowMeta getRowMeta();

    long getCacheTime();


}
