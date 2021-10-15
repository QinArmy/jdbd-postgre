package io.jdbd.mysql.stmt;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface StatementOption extends io.jdbd.vendor.stmt.StatementOption {

    /**
     * @return {@link Collections#emptyMap()} or a modified map.
     */
    Map<String, QueryAttr> getCommonAttrs();

    /**
     * @return {@link Collections#emptyList()} or a modified list.
     */
    List<Map<String, QueryAttr>> getAttrGroupList();


}
