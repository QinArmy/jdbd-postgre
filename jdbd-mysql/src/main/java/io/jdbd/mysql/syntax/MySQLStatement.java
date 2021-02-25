package io.jdbd.mysql.syntax;

import io.jdbd.vendor.statement.SQLStatement;

public interface MySQLStatement extends SQLStatement {


    boolean isLoadInfile();

}
