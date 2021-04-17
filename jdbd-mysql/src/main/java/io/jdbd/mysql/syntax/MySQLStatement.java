package io.jdbd.mysql.syntax;

import io.jdbd.vendor.stmt.SQLStatement;

public interface MySQLStatement extends SQLStatement {


    boolean isLoadInfile();

}
