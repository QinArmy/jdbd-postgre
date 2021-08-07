package io.jdbd.vendor.syntax;

import java.util.List;

public interface SQLStatement {

    /**
     * @return a unmodified list
     */
    List<String> getStaticSql();

    int getParamCount();

    String getSql();

}
