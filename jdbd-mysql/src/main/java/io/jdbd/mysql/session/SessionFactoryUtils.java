package io.jdbd.mysql.session;

import io.jdbd.PropertyException;
import io.jdbd.mysql.protocol.authentication.*;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.vendor.conf.Properties;

import java.util.*;

abstract class SessionFactoryUtils {

    SessionFactoryUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return a unmodifiable map ,key : {@link AuthenticationPlugin#getProtocolPluginName()}.
     * @throws PropertyException throw when below key error:<ul>
     *                           <li>{@link PropertyKey#defaultAuthenticationPlugin}</li>
     *                           <li>{@link PropertyKey#authenticationPlugins}</li>
     *                           <li>{@link PropertyKey#disabledAuthenticationPlugins}</li>
     *                           </ul>
     * @see MySQLSessionAdjutant#obtainPluginClassMap()
     */
    static Map<String, Class<? extends AuthenticationPlugin>> createPluginClassMap(Properties<PropertyKey> properties)
            throws PropertyException {

        final Map<String, Class<? extends AuthenticationPlugin>> allPluginMap = createAllPluginMap();
        final Map<String, String> pluginMechanismMapping = createPluginMechanismMapping();

        final List<String> disabledPluginMechanismList = loadDisabledPluginClassNameList(properties, pluginMechanismMapping);
        final Set<String> pluginClassNameSet = properties.getPropertySet(PropertyKey.authenticationPlugins);

        final String defaultPluginName = properties.getOrDefault(PropertyKey.defaultAuthenticationPlugin);
        final String defaultMechanism = pluginMechanismMapping.get(defaultPluginName);

        if (disabledPluginMechanismList.isEmpty()
                && pluginClassNameSet.isEmpty()
                && allPluginMap.containsKey(defaultMechanism)) {
            return allPluginMap;
        }

        pluginClassNameSet.addAll(allPluginMap.keySet()); // append all plugin class name

        final Map<String, Class<? extends AuthenticationPlugin>> map;
        map = new HashMap<>((int) (allPluginMap.size() / 0.75F));

        byte defaultFound = 0;
        for (String pluginClassName : pluginClassNameSet) {
            String mechanism = pluginMechanismMapping.get(pluginClassName.trim());
            if (mechanism == null) {
                String message = String.format("Property[%s] value[%s] can't load plugin."
                        , PropertyKey.authenticationPlugins.getKey(), pluginClassName);
                throw new PropertyException(PropertyKey.authenticationPlugins.getKey(), message);
            }
            if (disabledPluginMechanismList.contains(mechanism)) {
                if (mechanism.equals(defaultMechanism)) {
                    defaultFound = -1;
                }
                continue;
            }
            if (mechanism.equals(defaultMechanism)) {
                defaultFound = 1;
            }
            map.put(mechanism, allPluginMap.get(mechanism));
        }

        if (defaultFound == 0) {
            String message = String.format("%s[%s] not fond."
                    , PropertyKey.defaultAuthenticationPlugin.getKey(), defaultPluginName);
            throw new PropertyException(PropertyKey.defaultAuthenticationPlugin.getKey(), message);
        } else if (defaultFound == -1) {
            String message = String.format("%s[%s] disable."
                    , PropertyKey.defaultAuthenticationPlugin.getKey(), defaultPluginName);
            throw new PropertyException(PropertyKey.defaultAuthenticationPlugin.getKey(), message);
        }
        return MySQLCollections.unmodifiableMap(map);
    }


    /*################################## blow private static method ##################################*/


    /**
     * @return a unmodifiable map <ul>
     * <li>key:{@link AuthenticationPlugin#getProtocolPluginName()}</li>
     * </ul>
     * @see #createPluginClassMap(Properties)
     */
    private static Map<String, Class<? extends AuthenticationPlugin>> createAllPluginMap() {
        Map<String, Class<? extends AuthenticationPlugin>> map = new HashMap<>(8);

        map.put(MySQLNativePasswordPlugin.PLUGIN_NAME, MySQLNativePasswordPlugin.class);
        map.put(CachingSha2PasswordPlugin.PLUGIN_NAME, CachingSha2PasswordPlugin.class);
        map.put(MySQLClearPasswordPlugin.PLUGIN_NAME, MySQLClearPasswordPlugin.class);
        map.put(MySQLOldPasswordPlugin.PLUGIN_NAME, MySQLOldPasswordPlugin.class);

        map.put(Sha256PasswordPlugin.PLUGIN_NAME, Sha256PasswordPlugin.class);

        return Collections.unmodifiableMap(map);
    }

    /**
     * @return a unmodifiable map <ul>
     * <li>{@link AuthenticationPlugin#getProtocolPluginName()} or {@link AuthenticationPlugin} class name</li>
     * <li>{@link AuthenticationPlugin#getProtocolPluginName()}</li>
     * </ul>
     * @see #createPluginClassMap(Properties)
     */
    private static Map<String, String> createPluginMechanismMapping() {
        Map<String, String> map = new HashMap<>((int) (10 / 0.75F));

        map.put(MySQLNativePasswordPlugin.PLUGIN_NAME, MySQLNativePasswordPlugin.PLUGIN_NAME);
        map.put(MySQLNativePasswordPlugin.class.getName(), MySQLNativePasswordPlugin.PLUGIN_NAME);

        map.put(CachingSha2PasswordPlugin.PLUGIN_NAME, CachingSha2PasswordPlugin.PLUGIN_NAME);
        map.put(CachingSha2PasswordPlugin.class.getName(), CachingSha2PasswordPlugin.PLUGIN_NAME);

        map.put(MySQLClearPasswordPlugin.PLUGIN_NAME, MySQLClearPasswordPlugin.PLUGIN_NAME);
        map.put(MySQLClearPasswordPlugin.class.getName(), MySQLClearPasswordPlugin.PLUGIN_NAME);

        map.put(MySQLOldPasswordPlugin.PLUGIN_NAME, MySQLOldPasswordPlugin.PLUGIN_NAME);
        map.put(MySQLOldPasswordPlugin.class.getName(), MySQLOldPasswordPlugin.PLUGIN_NAME);

        map.put(Sha256PasswordPlugin.PLUGIN_NAME, Sha256PasswordPlugin.PLUGIN_NAME);
        map.put(Sha256PasswordPlugin.class.getName(), Sha256PasswordPlugin.PLUGIN_NAME);

        return Collections.unmodifiableMap(map);
    }

    /**
     * @return a unmodifiable list,element is {@link AuthenticationPlugin#getProtocolPluginName()}.
     * @see #createPluginClassMap(Properties)
     */
    private static List<String> loadDisabledPluginClassNameList(Properties<PropertyKey> properties
            , final Map<String, String> pluginNameMapping) throws PropertyException {

        String string = properties.getProperty(PropertyKey.disabledAuthenticationPlugins);
        if (!MySQLStringUtils.hasText(string)) {
            return Collections.emptyList();
        }
        String[] mechanismArray = string.split(",");
        List<String> list = new ArrayList<>(mechanismArray.length);
        for (String mechanismOrClassName : mechanismArray) {
            mechanismOrClassName = mechanismOrClassName.trim();
            String mechanism = pluginNameMapping.get(mechanismOrClassName);
            if (mechanism == null) {
                String message = String.format("Property[%s] value[%s] isn' mechanism or class name.."
                        , PropertyKey.disabledAuthenticationPlugins.getKey(), mechanismOrClassName);
                throw new PropertyException(PropertyKey.disabledAuthenticationPlugins.getKey(), message);
            }
            list.add(mechanism);
        }
        return MySQLCollections.unmodifiableList(list);
    }


}
