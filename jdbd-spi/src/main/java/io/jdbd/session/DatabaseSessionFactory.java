package io.jdbd.session;

import org.reactivestreams.Publisher;


public interface DatabaseSessionFactory extends Closeable {


    Publisher<LocalDatabaseSession> localSession();

    Publisher<RmDatabaseSession> rmSession();


    /**
     * @return database product name.
     */
    String productName();


}
