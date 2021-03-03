package io.jdbd.pool;

import io.jdbd.JdbdSessionFactory;

public interface ReadWriteSplittingSessionFactory extends JdbdSessionFactory {

    JdbdSessionFactory getPrimaryFactory();

}
