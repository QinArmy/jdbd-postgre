package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.conf.MySQLUrl;
import org.testng.annotations.Test;

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


}
