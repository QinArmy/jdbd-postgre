package io.jdbd.postgre.protocol.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

public class JdbcTests {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcTests.class);


    @Test
    public void connect() throws Exception {
        String url = "jdbc:postgresql://localhost:5432/army_test";

        Properties properties = new Properties();
        properties.put("user", "postgres");
        properties.put("password", "ITvtwm8341");

        try (Connection conn = DriverManager.getConnection(url, properties)) {
            try (Statement stmt = conn.createStatement()) {
                LOG.info("test success {}", stmt.execute("SELECT 1 AS tests"));
            }
        }

    }


}
