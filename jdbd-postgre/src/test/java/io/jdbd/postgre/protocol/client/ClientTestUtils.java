package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.config.PostgreUrl;
import org.qinarmy.env.Environment;
import org.qinarmy.env.ImmutableMapEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class ClientTestUtils {

    protected ClientTestUtils() {
        throw new UnsupportedOperationException();
    }


    public static PostgreUrl createUrl(Map<String, String> propertiesMap) {
        String url = "jdbc:postgresql://localhost:5433/army_test";
        Map<String, String> properties = new HashMap<>();
        properties.put("user", "army_w");
        properties.put("password", "army123");
        properties.putAll(propertiesMap);

        return PostgreUrl.create(url, properties);
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

    public static Path getTestResourcesPath() {
        return Paths.get(getModulePath().toString(), "src/test/resources");
    }

    private static Environment loadTestConfig() {
        final Path path = Paths.get(getTestResourcesPath().toString(), "testConfig.properties");
        Map<String, String> map;
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            Properties properties = new Properties();
            try (InputStream in = Files.newInputStream(path, StandardOpenOption.READ)) {
                properties.load(in);
                map = new HashMap<>((int) (properties.size() / 0.75F));
                for (Object key : properties.keySet()) {
                    String k = key.toString();
                    map.put(k, properties.getProperty(k));
                }
            } catch (IOException e) {
                throw new RuntimeException(String.format("load %s failure.", path), e);
            }

        } else {
            map = Collections.emptyMap();
        }
        return ImmutableMapEnvironment.create(map);
    }


}
