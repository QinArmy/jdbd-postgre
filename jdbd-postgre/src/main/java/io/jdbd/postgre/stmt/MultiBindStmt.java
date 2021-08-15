package io.jdbd.postgre.stmt;

import java.util.List;

public interface MultiBindStmt {

    List<BindableStmt> getGroup();

    int getTimeout();

}
