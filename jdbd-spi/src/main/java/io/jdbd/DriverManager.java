package io.jdbd;

import io.jdbd.lang.Nullable;
import io.jdbd.session.DatabaseSessionFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class DriverManager {

    private DriverManager() {
        throw new UnsupportedOperationException();
    }


    private static final ConcurrentMap<Class<? extends io.jdbd.Driver>, Driver> DRIVER_MAP = new ConcurrentHashMap<>();

    static {
        reload();
    }


    public static int reload() {
        return reload(Thread.currentThread().getContextClassLoader());
    }

    public static int reload(@Nullable final ClassLoader loader) {
        final PrivilegedAction<Integer> action = () -> doReload(loader);
        return AccessController.doPrivileged(action);
    }

    public static Collection<Driver> getDrivers() {
        return DRIVER_MAP.values();
    }


    /**
     * @throws NotFoundDriverException          when not found any driver for url.
     * @throws io.jdbd.config.UrlException      when url error.
     * @throws io.jdbd.config.PropertyException when properties error.
     */
    public static DatabaseSessionFactory createSessionFactory(final String url, final Map<String, String> properties) {
        return findTargetDriver(url)
                .createSessionFactory(url, properties);

    }


    /**
     * <p>
     * This method is designed for poll session vendor developer,so application developer shouldn't invoke this method
     * and use {@link #createSessionFactory(String, Map)} method.
     * </p>
     *
     * <p>  This method returning {@link DatabaseSessionFactory} has below feature.
     *     <ul>
     *         <li>{@link DatabaseSessionFactory#getTxSession()} returning instance is {@link io.jdbd.pool.PoolTxDatabaseSession} instance</li>
     *         <li>{@link DatabaseSessionFactory#getXaSession()} returning instance is {@link io.jdbd.pool.PoolXaDatabaseSession} instance</li>
     *     </ul>
     * </p>
     *
     * @throws NotFoundDriverException          when not found any driver for url.
     * @throws io.jdbd.config.UrlException      when url error.
     * @throws io.jdbd.config.PropertyException when properties error.
     */
    public static DatabaseSessionFactory forPoolVendor(final String url, final Map<String, String> properties)
            throws JdbdNonSQLException {
        return findTargetDriver(url)
                .forPoolVendor(url, properties);
    }

    /*################################## blow private static method ##################################*/


    private static Driver findTargetDriver(final String url) throws NotFoundDriverException {
        Driver targetDriver = null;
        for (Driver driver : DRIVER_MAP.values()) {
            if (driver.acceptsUrl(url)) {
                targetDriver = driver;
                break;
            }
        }
        if (targetDriver == null) {

            throw new NotFoundDriverException(url);
        }
        return targetDriver;
    }


    private static int doReload(@Nullable final ClassLoader loader) {
        try {
            final Enumeration<URL> enumeration;
            final String name = "META-INF/jdbd/io.jdbd.Driver";
            if (loader == null) {
                enumeration = ClassLoader.getSystemResources(name);
            } else {
                enumeration = loader.getResources(name);
            }

            int driverCount = 0;
            while (enumeration.hasMoreElements()) {
                driverCount += loadDriverInstances(enumeration.nextElement());
            }
            return driverCount;
        } catch (IOException e) {
            //no bug and no security,never here
            throw new RuntimeException(e);
        }
    }

    private static int loadDriverInstances(final URL url) {
        final Charset charset = StandardCharsets.UTF_8;
        int driverCount = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), charset))) {
            String line;
            Driver driver;
            while ((line = reader.readLine()) != null) {
                driver = getDriverInstance(line);
                if (driver != null && DRIVER_MAP.putIfAbsent(driver.getClass(), driver) == null) {
                    driverCount++;
                }
            }
        } catch (Throwable e) {
            //  don't follow io.jdbd.Driver contract,so ignore this url.
        }
        return driverCount;
    }

    @Nullable
    private static Driver getDriverInstance(final String className) {
        Driver instance;
        try {
            final Class<?> driverClass;
            driverClass = Class.forName(className);
            final Constructor<?> constructor = driverClass.getDeclaredConstructor();
            final Method method = driverClass.getMethod("getInstance");
            final int methodMod = method.getModifiers();
            if (Driver.class.isAssignableFrom(driverClass)
                    && Modifier.isPrivate(constructor.getModifiers())
                    && Modifier.isPublic(methodMod)
                    && Modifier.isStatic(methodMod)
                    && method.getReturnType() == driverClass) {
                instance = (Driver) method.invoke(null);
            } else {
                instance = null;
            }
        } catch (Throwable e) {
            // don't follow io.jdbd.Driver contract,so ignore.
            instance = null;
        }
        return instance;
    }


}
