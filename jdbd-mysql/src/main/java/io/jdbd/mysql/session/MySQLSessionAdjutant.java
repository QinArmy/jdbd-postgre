package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.vendor.session.SessionAdjutant;

import java.util.Map;

public interface MySQLSessionAdjutant extends SessionAdjutant<PropertyKey> {

    @Override
    MySQLUrl obtainUrl();

    /**
     * <p>
     * return a enabled plugin name map.
     *     <ul>
     *         <li>key : {@link AuthenticationPlugin#getProtocolPluginName()}</li>
     *         <li>value : {@link AuthenticationPlugin} class </li>
     *     </ul>
     * </p>
     *
     * @return a unmodifiable map.
     * @see PropertyKey#defaultAuthenticationPlugin
     * @see PropertyKey#authenticationPlugins
     * @see PropertyKey#disabledAuthenticationPlugins
     */
    Map<String, Class<? extends AuthenticationPlugin>> obtainPluginClassMap();


}
