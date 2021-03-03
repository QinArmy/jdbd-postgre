package io.jdbd.vendor.statement;

public interface PrepareWrapper {

    String getSql();

    /**
     * @return negative or fetch size, if zero ignore.
     */
    int getFetchSize();
}
