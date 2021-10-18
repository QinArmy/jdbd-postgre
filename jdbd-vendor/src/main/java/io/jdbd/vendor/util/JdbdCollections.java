package io.jdbd.vendor.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class JdbdCollections extends io.qinarmy.util.CollectionUtils {

    protected JdbdCollections() {
        throw new UnsupportedOperationException();
    }


    /**
     * @return a modified map
     */
    public static Map<String, String> loadProperties(final Path path) throws IOException {

        try (InputStream in = Files.newInputStream(path, StandardOpenOption.READ)) {
            final Properties properties = new Properties();
            properties.load(in);
            final Map<String, String> map = new HashMap<>((int) (properties.size() / 0.75F));
            for (Object key : properties.keySet()) {
                String k = key.toString();
                map.put(k, properties.getProperty(k));
            }
            return map;
        }

    }


}
