package io.jdbd.vendor.stmt;

import java.util.List;

public interface GroupStmt extends Stmt {

    List<String> getSqlGroup();

}
