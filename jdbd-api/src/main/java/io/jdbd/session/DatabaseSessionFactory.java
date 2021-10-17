package io.jdbd.session;

import io.jdbd.ProductFamily;
import org.reactivestreams.Publisher;


public interface DatabaseSessionFactory {


    Publisher<TxDatabaseSession> getTxSession();

    Publisher<XaDatabaseSession> getXaSession();


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
     * <p>  return driver name follow below:
     *     <ul>
     *         <li>If developer of implementation is database vendor,then should be database product name(eg:MySQL,DB2)</li>
     *         <li>Else ,then should be driver class name(eg:io.jdbd.mysql.Driver) </li>
     *     </ul>
     * </p>
     */
    String getDriverName();

    ProductFamily getProductFamily();


}
