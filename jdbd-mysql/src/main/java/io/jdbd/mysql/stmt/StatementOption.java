package io.jdbd.mysql.stmt;

import java.util.Collections;
import java.util.Map;

public interface StatementOption extends io.jdbd.vendor.stmt.StatementOption {

    /**
     * @return {@link Collections#emptyMap()} or  a modified map.
     */
    Map<String, QueryAttr> getAttrGroup();



}
