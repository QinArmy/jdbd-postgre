package io.jdbd.mysql.stmt;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLType;

public interface AttrStatement {

    /**
     * @throws io.jdbd.JdbdException bind error.
     */
    void bindQueryAttr(String name, MySQLType type, @Nullable Object value);


}
