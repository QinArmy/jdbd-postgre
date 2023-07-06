package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.StmtOption;

import java.util.Collections;
import java.util.Map;

public interface MySQLStmtOption extends StmtOption {

    /**
     * @return {@link Collections#emptyMap()} or  a modified map.
     */
    Map<String, QueryAttr> getAttrGroup();


}
