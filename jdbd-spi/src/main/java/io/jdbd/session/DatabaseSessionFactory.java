package io.jdbd.session;

import org.reactivestreams.Publisher;


public interface DatabaseSessionFactory extends Closeable {


    Publisher<LocalDatabaseSession> localSession();

    Publisher<RmDatabaseSession> globalSession();


    /**
     * @return database product name.
     */
    String getProductName();


}
