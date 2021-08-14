package io.jdbd.vendor.stmt;

import java.util.List;

public interface GroupStmt {

    List<String> getSqlGroup();

    int getTimeout();
}
