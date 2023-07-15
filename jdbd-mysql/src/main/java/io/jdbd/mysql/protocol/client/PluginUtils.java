package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.env.Environment;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLStrings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

abstract class PluginUtils {

    private PluginUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger LOG = LoggerFactory.getLogger(PluginUtils.class);

    private static final Map<String, String> PLUGIN_MECHANISM_MAPPING = createPluginMechanismMapping();


    /*
     * not java-doc
     * @see io.jdbd.mysql.protocol.client.MySQLConnectionTask
     */
    static String getDefaultMechanism(Environment env) {
        String defaultMechanism;
        defaultMechanism = PLUGIN_MECHANISM_MAPPING.get(
                env.getOrDefault(MySQLKey.DEFAULT_AUTHENTICATION_PLUGIN));
        // here can't be null ,because @see #createPluginClassMap
        return Objects.requireNonNull(defaultMechanism, "defaultMechanism");
    }

    /**
     * @return a unmodifiable map ,key : {@link AuthenticationPlugin#getProtocolPluginName()}.
     * @throws JdbdException throw when below key error:<ul>
     *                       <li>{@link MySQLKey#DEFAULT_AUTHENTICATION_PLUGIN}</li>
     *                       <li>{@link MySQLKey#AUTHENTICATION_PLUGINS}</li>
     *                       <li>{@link MySQLKey#DISABLED_AUTHENTICATION_PLUGINS}</li>
     *                       </ul>
     */
    static Map<String, Class<? extends AuthenticationPlugin>> createPluginClassMap(final Environment env)
            throws JdbdException {

        final Map<String, Class<? extends AuthenticationPlugin>> allPluginMap = createAllPluginMap();

        final List<String> disabledMechanismList, enabledMechanismList;
        disabledMechanismList = loadMechanismList(env, MySQLKey.DISABLED_AUTHENTICATION_PLUGINS);
        enabledMechanismList = loadMechanismList(env, MySQLKey.AUTHENTICATION_PLUGINS);

        final String defaultPluginName = env.getOrDefault(MySQLKey.DEFAULT_AUTHENTICATION_PLUGIN);
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
        map = MySQLCollections.hashMap((int) (allPluginMap.size() / 0.75F));

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
            String message = String.format("%s[%s] not fond.", MySQLKey.DISABLED_AUTHENTICATION_PLUGINS,
                    defaultPluginName);
            throw new JdbdException(message);
        } else if (defaultFound == -1) {
            String message = String.format("%s[%s] disable.", MySQLKey.DISABLED_AUTHENTICATION_PLUGINS,
                    defaultPluginName);
            throw new JdbdException(message);
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
     * @see #createPluginClassMap(Environment)
     */
    private static Map<String, Class<? extends AuthenticationPlugin>> createAllPluginMap() {
        final Map<String, Class<? extends AuthenticationPlugin>> map = MySQLCollections.hashMap(8);

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
     * @see #createPluginClassMap(Environment)
     */
    private static Map<String, String> createPluginMechanismMapping() {
        final Map<String, String> map = MySQLCollections.hashMap((int) (10 / 0.75F));

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


    private static List<String> loadMechanismList(final Environment env, final MySQLKey<String> key)
            throws JdbdException {
        String string = env.get(key);
        if (!MySQLStrings.hasText(string)) {
            return Collections.emptyList();
        }
        String[] classNameArray = string.split(",");
        final List<String> list = MySQLCollections.arrayList(classNameArray.length);
        String mechanism;
        for (String className : classNameArray) {
            mechanism = PLUGIN_MECHANISM_MAPPING.get(className.trim());
            if (mechanism == null) {
                String message = String.format("Property[%s] value[%s] isn' %s implementation class name.",
                        key.name, className, AuthenticationPlugin.class.getName());
                throw new JdbdException(message);
            }
            list.add(mechanism);
        }
        return MySQLCollections.unmodifiableList(list);
    }


}
