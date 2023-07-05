package io.jdbd.session;

import io.jdbd.env.JdbdEnvironment;
import org.reactivestreams.Publisher;


public interface DatabaseSessionFactory extends Closeable {


    Publisher<LocalDatabaseSession> localSession();

    Publisher<RmDatabaseSession> globalSession();


    /**
     * @return database product name.
     */
    String productName();

    JdbdEnvironment environment();


}
