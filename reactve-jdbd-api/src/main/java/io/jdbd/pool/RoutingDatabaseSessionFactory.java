package io.jdbd.pool;

import io.jdbd.DatabaseSessionFactory;

public interface RoutingDatabaseSessionFactory  extends DatabaseSessionFactory {

    DatabaseSessionFactory getPrimaryFactory();

}
