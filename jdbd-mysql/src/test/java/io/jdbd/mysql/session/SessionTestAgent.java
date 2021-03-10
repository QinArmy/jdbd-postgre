package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.authentication.PluginUtils;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.vendor.conf.Properties;

import java.util.Map;

public class SessionTestAgent {


    /**
     * @see io.jdbd.vendor.session.SessionAdjutant
     * @see PluginUtils#createPluginClassMap(Properties)
     */
    public static Map<String, Class<? extends AuthenticationPlugin>> createPluginClassMap(
            Properties<PropertyKey> properties) {
        return PluginUtils.createPluginClassMap(properties);
    }


}
