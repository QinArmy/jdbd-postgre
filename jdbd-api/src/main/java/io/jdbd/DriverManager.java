package io.jdbd;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class DriverManager {

    private DriverManager() {
        throw new UnsupportedOperationException();
    }


    private static final ConcurrentMap<Class<? extends io.jdbd.Driver>, Driver> DRIVER_MAP = new ConcurrentHashMap<>();


    public static boolean registerDriver(final Class<?> driverType) throws IllegalArgumentException {
        if (!io.jdbd.Driver.class.isAssignableFrom(driverType)) {
            throw new IllegalArgumentException(String.format("driverClass[%s] isn't %s type."
                    , driverType.getName(), io.jdbd.Driver.class.getName()));
        }
        @SuppressWarnings("unchecked") final Class<? extends io.jdbd.Driver> driverClass = (Class<? extends io.jdbd.Driver>) driverType;
        if (DRIVER_MAP.containsKey(driverClass)) {
            return false;
        }
        try {

            final Method method = driverClass.getDeclaredMethod("getInstance");

            final int modifier = method.getModifiers();
            if (Modifier.isPublic(modifier)
                    && Modifier.isStatic(modifier)
                    && method.getReturnType() == driverClass) {
                final io.jdbd.Driver driver;
                driver = (io.jdbd.Driver) method.invoke(null);
                if (driver == null) {
                    String m = String.format("public static %s getInstance() method of %s return null."
                            , driverType.getName(), driverType.getName());
                    throw new IllegalArgumentException(m);
                }
                return DRIVER_MAP.putIfAbsent(driverClass, driver) == null;
            } else {
                String m = String.format("Not found public static %s getInstance() method in %s"
                        , driverType.getName(), driverType.getName());
                throw new IllegalArgumentException(m);
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            String m = String.format("%s.getInstance() invoker error.%s", driverClass.getName(), e.getMessage());
            throw new IllegalArgumentException(m, e);
        }

    }


    /**
     * @throws NotFoundDriverException          when not found any driver for url.
     * @throws io.jdbd.config.UrlException      when url error.
     * @throws io.jdbd.config.PropertyException when properties error.
     */
    public static DatabaseSessionFactory createSessionFactory(final String url, final Map<String, String> properties)
            throws JdbdNonSQLException {
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
     */
    public static DatabaseSessionFactory forPoolVendor(final String url, final Map<String, String> properties)
            throws JdbdNonSQLException {
        return findTargetDriver(url)
                .forPoolVendor(url, properties);
    }


    private static Driver findTargetDriver(String url) throws NotFoundDriverException {
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


}
