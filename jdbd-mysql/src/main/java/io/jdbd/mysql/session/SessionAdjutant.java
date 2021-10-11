package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.protocol.conf.MySQLHost;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.vendor.session.ISessionAdjutant;

import java.util.Map;

public interface SessionAdjutant extends ISessionAdjutant<MyKey, MySQLHost> {

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
     * @see MyKey#defaultAuthenticationPlugin
     * @see MyKey#authenticationPlugins
     * @see MyKey#disabledAuthenticationPlugins
     */
    Map<String, Class<? extends AuthenticationPlugin>> obtainPluginClassMap();

    /**
     * @see MyKey#maxAllowedPacket
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    int maxAllowedPayload();

}
