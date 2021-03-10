package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.vendor.conf.Properties;

import java.util.Map;

public class SessionTestAgent {


    /**
     * @see io.jdbd.vendor.session.SessionAdjutant
     * @see SessionFactoryUtils#createPluginClassMap(Properties)
     */
    public static Map<String, Class<? extends AuthenticationPlugin>> createPluginClassMap(
            Properties<PropertyKey> properties) {
        return SessionFactoryUtils.createPluginClassMap(properties);
    }


}
