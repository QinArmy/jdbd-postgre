package io.jdbd;

import io.jdbd.xa.XaDatabaseSession;
import org.reactivestreams.Publisher;

import java.util.Map;

public interface DatabaseSessionFactory {


    Publisher<TxDatabaseSession> getTxSession();

    Publisher<XaDatabaseSession> getXaSession();


    /**
     * @see DriverManager#createSessionFactory(String, Map)
     */
    String getUrl();

    /**
     * @return a unmodifiable map
     * @see DriverManager#createSessionFactory(String, Map)
     */
    Map<String, String> getProperties();


    /**
     * Retrieves the driver's major version number. Initially this should be 1.
     *
     * @return this driver's major version number
     */
    int getMajorVersion();

    /**
     * Gets the driver's minor version number. Initially this should be 0.
     *
     * @return this driver's minor version number
     */
    int getMinorVersion();

    /**
     * <p>
     * Return name of driver,popularly is database name(eg:MySQL) or class name(eg:io.jdbd.mysql.Driver)
     * </p>
     */
    String getDriverName();

}
