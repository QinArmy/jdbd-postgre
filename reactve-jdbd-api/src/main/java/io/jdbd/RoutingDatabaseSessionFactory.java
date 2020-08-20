package io.jdbd;

public interface RoutingDatabaseSessionFactory  extends DatabaseSessionFactory{

    DatabaseSessionFactory getPrimaryFactory();

}
