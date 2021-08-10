package io.jdbd.postgre.protocol.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class JdbcTests {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcTests.class);


    @Test
    public void connect() throws Exception {
        String url = "jdbc:postgresql://localhost:5432/army_test?options=-c%20IntervalStyle=iso_8601";

        Properties properties = new Properties();
        properties.put("user", "army_w");
        properties.put("password", "army123");
        properties.put("preferQueryMode", "simple");
        properties.put("currentSchema", "army");

        try (Connection conn = DriverManager.getConnection(url, properties)) {
            try (Statement stmt = conn.createStatement()) {
                String sql;
                sql = "SET IntervalStyle = 'postgres'";
                LOG.info("test success {}", stmt.executeUpdate(sql));
                sql = "UPDATE table_name AS t SET create_time = '2021-08-08 10:54:30' WHERE t.id = 1";
                LOG.info("test success {}", stmt.execute(sql));
                sql = "SHOW IntervalStyle";
                try (ResultSet resultSet = stmt.executeQuery(sql)) {
                    while (resultSet.next()) {
                        LOG.info("{}", resultSet.getString(1));
                    }
                }


            }
        }

    }


}
