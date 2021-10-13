package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamValue;

import java.util.List;
import java.util.Map;

public interface MySQLParamBatchStmt<T extends ParamValue> extends ParamBatchStmt<T>, MySQLParamSingleStmt {

    /**
     * <p>
     * This method return query attributes representing common query attributes for each bind group.
     * </p>
     *
     * @return a unmodified map
     */
    @Override
    Map<String, QueryAttr> getQueryAttrs();

    /**
     * <p>
     * This method return list size always equals {@link #getGroupList()} size.
     * </p>
     *
     * @return a unmodified list
     */
    List<Map<String, QueryAttr>> getQueryAttrGroup();

}
