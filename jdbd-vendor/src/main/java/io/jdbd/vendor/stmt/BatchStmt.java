package io.jdbd.vendor.stmt;

import java.util.List;

public interface BatchStmt extends Stmt {

    List<String> getSqlGroup();

}
