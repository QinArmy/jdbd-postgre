package io.jdbd.vendor.stmt;

import java.util.List;

public interface SQLStatement {

    List<String> getStaticSql();

    int getParamCount();

    String getSql();

}
