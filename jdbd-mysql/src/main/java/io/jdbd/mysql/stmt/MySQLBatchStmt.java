package io.jdbd.mysql.stmt;

import java.util.List;
import java.util.Map;

public interface MySQLBatchStmt extends MySQLStmt {

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
     * This method return list size always equals {@code #getGroupList()} size.
     * </p>
     *
     * @return a unmodified list
     */
    List<Map<String, QueryAttr>> getQueryAttrGroup();

}
