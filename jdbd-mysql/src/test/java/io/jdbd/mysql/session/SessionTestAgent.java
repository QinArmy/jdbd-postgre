package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.authentication.PluginUtils;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.session.ISessionAdjutant;

import java.util.Map;

public class SessionTestAgent {


    /**
     * @see ISessionAdjutant
     * @see PluginUtils#createPluginClassMap(Properties)
     */
    public static Map<String, Class<? extends AuthenticationPlugin>> createPluginClassMap(
            Properties<MyKey> properties) {
        return PluginUtils.createPluginClassMap(properties);
    }


}
