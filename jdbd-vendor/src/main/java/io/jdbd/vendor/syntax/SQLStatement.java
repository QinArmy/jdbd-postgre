package io.jdbd.vendor.syntax;

import java.util.List;

public interface SQLStatement {

    List<String> getStaticSql();

    int getParamCount();

    String getSql();

}
