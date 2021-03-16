package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@Test(enabled = false)
public abstract class ClientTestUtils {

    protected ClientTestUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Path SERVER_PUBLIC_KEY_PATH = Paths.get(ClientTestUtils.getTestResourcesPath().toString(), "my-local/mysql-server/public_key.pem");


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

    public static Path getServerPublicKeyPath() {
        return SERVER_PUBLIC_KEY_PATH;
    }

    public static boolean existsServerPublicKey() {
        return Files.exists(SERVER_PUBLIC_KEY_PATH);
    }

    public static void appendZoneConfig(Map<String, String> map) {
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime utcNow = now.withOffsetSameInstant(ZoneOffset.UTC);

        final ZoneOffset clientZoneOffset = MySQLTimeUtils.systemZoneOffset(), databaseZoneOffset;
        if (utcNow.equals(now)) {
            databaseZoneOffset = ZoneOffset.of("+08:00");
        } else {
            databaseZoneOffset = ZoneOffset.of("+00:00");
        }
        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .appendOffset("+HH:MM", "+00:00")
                .toFormatter(Locale.ENGLISH);
        String variable = String.format("@@SESSION.time_zone='%s'", formatter.format(databaseZoneOffset));
        appendSessionVariable(map, variable);
        map.put(PropertyKey.connectionTimeZone.getKey(), clientZoneOffset.getId());
    }

    public static void appendSessionVariable(Map<String, String> map, String pair) {
        String variables = map.get(PropertyKey.sessionVariables.getKey());

        if (MySQLStringUtils.hasText(variables)) {
            variables += ("," + pair);
            map.put(PropertyKey.sessionVariables.getKey(), variables);
        } else {
            map.put(PropertyKey.sessionVariables.getKey(), pair);
        }
    }

    public static Path getTestResourcesPath() {
        return Paths.get(getModulePath().toString(), "src/test/resources");
    }

    public static Charset getSystemFileCharset() {
        return Charset.forName(System.getProperty("file.encoding"));
    }


}
