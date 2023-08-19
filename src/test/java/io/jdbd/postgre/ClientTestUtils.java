package io.jdbd.postgre;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class ClientTestUtils {

    protected ClientTestUtils() {
        throw new UnsupportedOperationException();
    }


    public static PgUrl createUrl(Map<String, String> propertiesMap) {
        String url = "jdbc:postgresql://localhost:5432/army_test";
        Map<String, String> properties = new HashMap<>();

        properties.put("user", "army_w");
        properties.put("password", "army123");
        properties.put("sslmode", "DISABLED");
        properties.put(PgKey0.ApplicationName.getKey(), "jdbd-postgre-test");

        properties.put(PgKey0.currentSchema.getKey(), "army");
        properties.putAll(propertiesMap);

        return PgUrl.create(url, properties);
    }

    public static Path getModulePath() {
        Path path = Paths.get(System.getProperty("user.dir"));
        if (!path.toString().endsWith("jdbd-postgre")) {
            path = Paths.get(path.toString(), "jdbd-postgre");
        }
        return path;
    }

    public static Path getTestMyLocalPath() {
        Path modelPath = getModulePath();
        return Paths.get(modelPath.toString(), "target/test-classes/my-local");
    }

    public static Path getClassPath() {
        URL url = ClientTestUtils.class.getClassLoader().getResource("");
        if (url == null) {
            throw new RuntimeException("no class path");
        }
        String urlText = url.toString();
        String prefix = "file:";
        Path path;
        if (urlText.startsWith(prefix)) {
            path = Paths.get(urlText.substring(prefix.length()));
        } else {
            path = Paths.get(urlText);
        }
        return path.toAbsolutePath();
    }

    public static Path getTestResourcesPath() {
        return Paths.get(getModulePath().toString(), "src/test/resources");
    }

    /**
     * @return a modifiable map.
     */
    public static Map<String, String> loadTestConfigMap() {
        final Path path = Paths.get(getTestResourcesPath().toString(), "config", "postgre-test.properties");
        try (InputStream in = Files.newInputStream(path, StandardOpenOption.READ)) {
            Properties properties = new Properties();
            properties.load(in);
            final Map<String, String> propMap = new HashMap<>((int) (properties.size() / 0.75F));
            for (Object key : properties.keySet()) {
                String k = key.toString();
                propMap.put(k, properties.getProperty(k));
            }
            return propMap;
        } catch (IOException e) {
            String m = String.format("%s config file error", path);
            throw new IllegalStateException(m, e);
        }
    }

}
