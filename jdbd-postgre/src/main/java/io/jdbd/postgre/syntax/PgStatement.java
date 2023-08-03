package io.jdbd.postgre.syntax;

import io.jdbd.vendor.syntax.SQLStatement;

public interface PgStatement extends SQLStatement {

    boolean isStandardConformingStrings();


}
