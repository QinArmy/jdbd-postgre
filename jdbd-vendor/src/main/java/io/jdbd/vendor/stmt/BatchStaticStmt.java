package io.jdbd.vendor.stmt;

import java.util.List;

public interface BatchStaticStmt {

    List<String> getSqlList();

    int getTimeout();

}
