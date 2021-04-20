package io.jdbd.vendor.stmt;

import java.util.List;

public interface ParamStmt extends StmtWrapper {

    /**
     * @return a unmodifiable list
     */
    List<? extends ParamValue> getParamGroup();


    /**
     * @return negative or fetch size, if zero ignore.
     */
    int getFetchSize();



}
