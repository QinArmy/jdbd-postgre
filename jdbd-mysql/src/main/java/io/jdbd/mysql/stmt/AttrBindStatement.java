package io.jdbd.mysql.stmt;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLType;
import io.jdbd.stmt.BindStatement;

public interface AttrBindStatement extends AttrStatement, BindStatement {

    /**
     * @throws io.jdbd.JdbdException bind error.
     */
    void bindAttr(String name, MySQLType type, @Nullable Object value);


}
