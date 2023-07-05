package io.jdbd.session;

import io.jdbd.DriverVersion;
import io.jdbd.ProductFamily;
import org.reactivestreams.Publisher;


public interface DatabaseSessionFactory {


    Publisher<LocalDatabaseSession> getTxSession();

    Publisher<RmDatabaseSession> getXaSession();


    DriverVersion getDriverVersion();

    ProductFamily getProductFamily();


}
