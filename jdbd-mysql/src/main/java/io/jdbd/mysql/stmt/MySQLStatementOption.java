package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.StatementOption;

import java.util.Collections;
import java.util.Map;

public interface MySQLStatementOption extends StatementOption {

    /**
     * @return {@link Collections#emptyMap()} or  a modified map.
     */
    Map<String, QueryAttr> getAttrGroup();



}
