package io.jdbd.vendor.stmt;

import java.util.List;

public interface GroupStmt extends StmtOptions {

    List<String> getSqlGroup();

}
