package io.jdbd.vendor;

public interface SQLStatement {

    byte[][] getStaticSql();

    int getParamCount();

    String getSql();

    int getParamPosition(int paramIndex);


}
