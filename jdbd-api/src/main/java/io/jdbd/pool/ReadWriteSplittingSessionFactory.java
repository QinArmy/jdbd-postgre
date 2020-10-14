package io.jdbd.pool;

import io.jdbd.DatabaseSessionFactory;

public interface ReadWriteSplittingSessionFactory extends DatabaseSessionFactory {

    DatabaseSessionFactory getPrimaryFactory();

}
