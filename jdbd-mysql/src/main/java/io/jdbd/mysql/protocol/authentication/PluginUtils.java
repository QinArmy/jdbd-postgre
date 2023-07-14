package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.env.Environment;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.session.SessionAdjutant;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.vendor.env.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class PluginUtils {

    PluginUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger LOG = LoggerFactory.getLogger(PluginUtils.class);

    private static final Map<String, String> PLUGIN_MECHANISM_MAPPING = createPluginMechanismMapping();


    /*
     * not java-doc
     * @see io.jdbd.mysql.protocol.client.MySQLConnectionTask
     */
    public static String getDefaultMechanism(Environment env) {
        String defaultMechanism;
        defaultMechanism = PLUGIN_MECHANISM_MAPPING.get(
                env.getOrDefault(MySQLKey.DEFAULT_AUTHENTICATION_PLUGIN));
        // here can't be null ,because @see #createPluginClassMap
        return Objects.requireNonNull(defaultMechanism, "defaultMechanism");
    }

    /**
     * @return a unmodifiable map ,key : {@link AuthenticationPlugin#getProtocolPluginName()}.
     * @throws PropertyException throw when below key error:<ul>
     *                           <li>{@link MyKey#defaultAuthenticationPlugin}</li>
     *                           <li>{@link MyKey#authenticationPlugins}</li>
     *                           <li>{@link MyKey#disabledAuthenticationPlugins}</li>
     *                           </ul>
     * @see SessionAdjutant#pluginClassMap()
     */
    public static Map<String, Class<? extends AuthenticationPlugin>> createPluginClassMap(Properties properties)
            throws PropertyException {

        final Map<String, Class<? extends AuthenticationPlugin>> allPluginMap = createAllPluginMap();

        final List<String> disabledMechanismList = loadDisabledPluginMechanismList(properties);
        final List<String> enabledMechanismList = loadEnabledPluginMechanismList(properties);

        final String defaultPluginName = properties.getOrDefault(MyKey.defaultAuthenticationPlugin);
        final String defaultMechanism = PLUGIN_MECHANISM_MAPPING.get(defaultPluginName);

        if (disabledMechanismList.isEmpty()
                && enabledMechanismList.isEmpty()
                && allPluginMap.containsKey(defaultMechanism)) {
            return allPluginMap;
        }

        final Collection<String> mechanismCollection;
        if (enabledMechanismList.isEmpty()) {
            mechanismCollection = allPluginMap.keySet();
        } else {
            mechanismCollection = enabledMechanismList;
        }

        final Map<String, Class<? extends AuthenticationPlugin>> map;
        map = new HashMap<>((int) (allPluginMap.size() / 0.75F));

        byte defaultFound = 0;
        for (String mechanism : mechanismCollection) {
            if (disabledMechanismList.contains(mechanism)) {
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
                    , MyKey.defaultAuthenticationPlugin.getKey(), defaultPluginName);
            throw new PropertyException(MyKey.defaultAuthenticationPlugin.getKey(), message);
        } else if (defaultFound == -1) {
            String message = String.format("%s[%s] disable."
                    , MyKey.defaultAuthenticationPlugin.getKey(), defaultPluginName);
            throw new PropertyException(MyKey.defaultAuthenticationPlugin.getKey(), message);
        }

        if (LOG.isTraceEnabled()) {
            int index = 0;
            StringBuilder builder = new StringBuilder("enabled authentication mechanisms:\n");
            for (String mechanism : map.keySet()) {
                index++;
                builder.append(index)
                        .append(" - ")
                        .append(mechanism)
                        .append("\n");
            }
            LOG.trace(builder.toString());
        }
        return MySQLCollections.unmodifiableMap(map);
    }

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
    private static List<String> loadDisabledPluginMechanismList(Properties properties)
            throws PropertyException {

        String string = properties.get(MyKey.disabledAuthenticationPlugins);
        if (!MySQLStrings.hasText(string)) {
            return Collections.emptyList();
        }
        String[] mechanismArray = string.split(",");
        List<String> list = new ArrayList<>(mechanismArray.length);
        for (String mechanismOrClassName : mechanismArray) {
            mechanismOrClassName = mechanismOrClassName.trim();
            String mechanism = PLUGIN_MECHANISM_MAPPING.get(mechanismOrClassName);
            if (mechanism == null) {
                String message = String.format("Property[%s] value[%s] isn' mechanism or class name.."
                        , MyKey.disabledAuthenticationPlugins.getKey(), mechanismOrClassName);
                throw new PropertyException(MyKey.disabledAuthenticationPlugins.getKey(), message);
            }
            list.add(mechanism);
        }
        return MySQLCollections.unmodifiableList(list);
    }

    /**
     * @return a unmodifiable list,element is {@link AuthenticationPlugin#getProtocolPluginName()}.
     * @see #createPluginClassMap(Properties)
     */
    private static List<String> loadEnabledPluginMechanismList(Properties properties)
            throws PropertyException {
        String string = properties.get(MyKey.authenticationPlugins);
        if (!MySQLStrings.hasText(string)) {
            return Collections.emptyList();
        }
        String[] classNameArray = string.split(",");
        final List<String> list = new ArrayList<>(classNameArray.length);
        for (String className : classNameArray) {
            String mechanism = PLUGIN_MECHANISM_MAPPING.get(className.trim());
            if (mechanism == null) {
                String message = String.format("Property[%s] value[%s] isn' %s implementation class name.."
                        , MyKey.authenticationPlugins.getKey(), className, AuthenticationPlugin.class.getName());
                throw new PropertyException(MyKey.authenticationPlugins.getKey(), message);
            }
            list.add(mechanism);
        }
        return MySQLCollections.unmodifiableList(list);
    }


}
