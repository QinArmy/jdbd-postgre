package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.conf.MySQLUrl;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

public abstract class MySQLUrlUtils {

    protected MySQLUrlUtils() {
        throw new UnsupportedOperationException();
    }


    public static MySQLUrl build(Map<String, String> propertiesMap) {
        final ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(Instant.now());
        // PREFERRED ,DISABLED
        String url = "jdbc:mysql://localhost:3306/army";
        Map<String, String> properties = new HashMap<>();
        properties.put("user", "army_w");
        properties.put("password", "army123");

        properties.put("sslMode", "DISABLED");
        properties.put("detectCustomCollations", "true");
        properties.put("sessionVariables", String.format("time_zone='%s'", zoneOffset));

        properties.putAll(propertiesMap);
        return MySQLUrl.getInstance(url, properties);
    }
}
