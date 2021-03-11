package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.conf.MySQLUrl;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Test(enabled = false)
public abstract class ClientTestUtils {

    protected ClientTestUtils() {
        throw new UnsupportedOperationException();
    }


    public static MySQLUrl singleUrl(Map<String, String> propertiesMap) {
        // PREFERRED ,DISABLED
        String url = "jdbc:mysql://localhost:3306/army";
        Map<String, String> properties = new HashMap<>();
        properties.put("user", "army_w");
        properties.put("password", "army123");

        properties.putAll(propertiesMap);
        return MySQLUrl.getInstance(url, properties);
    }

    public static Path getModulePath() {
        Path path = Paths.get(System.getProperty("user.dir"));
        if (!path.toString().endsWith("jdbd-mysql")) {
            path = Paths.get(path.toString(), "jdbd-mysql");
        }
        return path;
    }

    public static Path getTestResourcesPath() {
        return Paths.get(getModulePath().toString(), "src/test/resources");
    }

    public static Charset getSystemFileCharset() {
        return Charset.forName(System.getProperty("file.encoding"));
    }


}
