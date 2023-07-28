package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.mysql.util.MySQLTimes;
import io.qinarmy.env.Environment;
import io.qinarmy.env.ImmutableMapEnvironment;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;
import java.util.Map;

@Test(enabled = false)
public abstract class ClientTestUtils {

    protected ClientTestUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Path SERVER_PUBLIC_KEY_PATH = Paths.get(ClientTestUtils.getTestResourcesPath().toString(), "my-local/mysql-server/public_key.pem");

    private static final Environment ENV = loadTestConfig();



    public static Path getModulePath() {
        Path path = Paths.get(System.getProperty("user.dir"));
        if (!path.toString().endsWith("jdbd-mysql")) {
            path = Paths.get(path.toString(), "jdbd-mysql");
        }
        return path;
    }

    public static Path getTestMyLocalPath() {
        Path modelPath = getModulePath();
        return Paths.get(modelPath.toString(), "target/test-classes/my-local");
    }

    public static Path getBigColumnTestPath() {
        return Paths.get(getTestMyLocalPath().toString(), "bigColumn");
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

        final ZoneOffset clientZoneOffset = MySQLTimes.systemZoneOffset(), databaseZoneOffset;
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
        map.put(MyKey.connectionTimeZone.getKey(), clientZoneOffset.getId());
    }

    public static void appendSessionVariable(Map<String, String> map, String pair) {
        String variables = map.get(MyKey.sessionVariables.getKey());

        if (MySQLStrings.hasText(variables)) {
            variables += ("," + pair);
            map.put(MyKey.sessionVariables.getKey(), variables);
        } else {
            map.put(MyKey.sessionVariables.getKey(), pair);
        }
    }

    public static Path getTestResourcesPath() {
        return Paths.get(getModulePath().toString(), "src/test/resources");
    }

    public static Charset getSystemFileCharset() {
        return Charset.forName(System.getProperty("file.encoding"));
    }


    public static Environment getTestConfig() {
        return ENV;
    }

    public static Map<String, Object> loadConfigMap() {
        final Path path = Paths.get(getTestResourcesPath().toString(), "mysql.properties");
        try {
            return MySQLCollections.loadProperties(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /*################################## blow private method ##################################*/


    private static Environment loadTestConfig() {
        return ImmutableMapEnvironment.create(loadConfigMap());
    }


}
