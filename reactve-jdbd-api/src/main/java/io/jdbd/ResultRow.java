package io.jdbd;

import io.jdbd.lang.Nullable;


public interface ResultRow {

    @Nullable
    Object getObject(String alias)throws ReactiveSQLException;

    @Nullable
    <T> T getObject(String alias, Class<T> columnClass)throws ReactiveSQLException;

}
