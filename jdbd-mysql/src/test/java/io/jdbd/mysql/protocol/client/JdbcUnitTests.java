package io.jdbd.mysql.protocol.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.Properties;

public class JdbcUnitTests {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUnitTests.class);

    private static final String URL = "jdbc:mysql://localhost:3306/army_test";

    @Test
    public void statement() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL, createProperties())) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CALL demoSp('army',0)");
            }
        }

    }

    @Test
    public void prepare() throws SQLException {
        Properties prop = new Properties(createProperties());
        prop.put("useServerPrepStmts", "true");

        try (Connection conn = DriverManager.getConnection(URL, prop)) {
            String sql = "CALL demoSp(?)";
            try (CallableStatement stmt = conn.prepareCall(sql)) {
                stmt.setString(1, "QinArmy");
//                stmt.registerOutParameter(1, MysqlType.INT);

                stmt.execute();
            }
        }
    }

    private static Properties createProperties() {
        Properties properties = new Properties();
        properties.put("user", "army_w");
        properties.put("password", "army123");
        properties.put("sslMode", "DISABLED");
        properties.put("useServerPrepStmts", "true");
        return properties;
    }


}
