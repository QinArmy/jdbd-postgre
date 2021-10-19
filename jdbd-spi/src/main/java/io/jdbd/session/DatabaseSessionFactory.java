package io.jdbd.session;

import io.jdbd.DriverVersion;
import io.jdbd.ProductFamily;
import org.reactivestreams.Publisher;


public interface DatabaseSessionFactory {


    Publisher<TxDatabaseSession> getTxSession();

    Publisher<XaDatabaseSession> getXaSession();


    DriverVersion getDriverVersion();

    ProductFamily getProductFamily();


}
