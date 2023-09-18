package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.result.ResultRowMeta;

import java.util.List;

interface ServerCacheStmt extends PostgreStmt {

    String stmtName();

    List<DataType> getParamOidList();

    ResultRowMeta getRowMeta();


}
