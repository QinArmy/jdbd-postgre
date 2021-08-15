package io.jdbd.vendor.syntax;

import java.util.List;

public interface SQLStatement {

    /**
     * @return a unmodified list
     */
    List<String> getStaticSql();

    @Deprecated
    int getParamCount();

    String getSql();

}
