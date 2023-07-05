package io.jdbd.session;

import io.jdbd.DriverVersion;
import io.jdbd.ProductFamily;
import org.reactivestreams.Publisher;


public interface DatabaseSessionFactory extends Closeable {


    Publisher<LocalDatabaseSession> localSession();

    Publisher<RmDatabaseSession> globalSession();


    DriverVersion getDriverVersion();

    ProductFamily getProductFamily();




}
