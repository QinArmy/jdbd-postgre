package io.jdbd;

import reactor.util.annotation.Nullable;


public interface ResultRow {

    @Nullable
    Object getObject(int indexBaseZero) throws ReactiveSQLException;

    @Nullable
    <T> T getObject(int indexBaseZero, Class<T> columnClass) throws ReactiveSQLException;

    @Nullable
    Object getObject(String alias) throws ReactiveSQLException;

    @Nullable
    <T> T getObject(String alias, Class<T> columnClass) throws ReactiveSQLException;

}
