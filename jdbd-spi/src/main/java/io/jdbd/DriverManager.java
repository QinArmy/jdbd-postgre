package io.jdbd;

import io.jdbd.lang.Nullable;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;

/**
 * @since 1.0
 */
abstract class DriverManager {

    private DriverManager() {
        throw new UnsupportedOperationException();
    }

    static Driver findDriver(final String jdbdUrl) throws JdbdException {
        try {
            final Enumeration<URL> enumeration;
            enumeration = Thread.currentThread().getContextClassLoader()
                    .getResources("META-INF/jdbd/io.jdbd.Driver");

            Driver driver = null;
            while (enumeration.hasMoreElements()) {
                driver = loadDriverInstance(enumeration.nextElement(), jdbdUrl);
                if (driver != null) {
                    break;
                }
            }

            if (driver == null) {
                throw new JdbdException(String.format("Not found driver for url %s", jdbdUrl));
            }
            return driver;
        } catch (Throwable e) {
            //no bug and no security,never here
            throw new JdbdException(e.getMessage(), e);
        }

    }


    @Nullable
    private static Driver loadDriverInstance(final URL url, final String jdbdUrl) throws JdbdException {
        final Charset charset = StandardCharsets.UTF_8;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), charset))) {
            String line;
            Driver driver = null;
            while ((line = reader.readLine()) != null) {
                driver = getDriverInstance(line);
                if (driver != null && driver.acceptsUrl(jdbdUrl)) {
                    break;
                }
            }
            return driver;
        } catch (Throwable e) {
            //  don't follow io.jdbd.Driver contract
            throw new JdbdException(e.getMessage(), e);
        }

    }

    @Nullable
    private static Driver getDriverInstance(final String className) {
        Driver instance;
        try {
            final Class<?> driverClass;
            driverClass = Class.forName(className);
            final Method method;
            method = driverClass.getMethod("getInstance");
            final int modifier = method.getModifiers();
            if (Driver.class.isAssignableFrom(driverClass)
                    && Modifier.isPublic(modifier)
                    && Modifier.isStatic(modifier)
                    && method.getParameterCount() == 0
                    && Driver.class.isAssignableFrom(method.getReturnType())) {
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
