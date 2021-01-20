package io.jdbd.mysql.protocol.conf;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.jdbc.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Matcher;

public class MySQLParserTests {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLParserTests.class);

    private static final String COMMON_URL = "jdbc:mysql://address=(host=kafka)(port=3435)(key2=value2),localhost:8080,( host  =  kosmo , port = 3306 ),( host  =  simonyi , port = 9987 ),zoro:3306,address=(host=myhost2)(port=2222)(key2=value2)/db?useSSL=true";


    @Test//(invocationCount = 100)
    public void simpleTest() throws Exception {
        long start = System.currentTimeMillis();
        MySQLUrlParser parser = MySQLUrlParser.parseConnectionString(COMMON_URL, Collections.singletonMap("user", "army"));
        for (HostInfo parsedHost : parser.getParsedHosts()) {
            LOG.info("parsedHost:{}", parsedHost.getHostPortPair());
        }
        LOG.info("cost {} ms", System.currentTimeMillis() - start);
    }

    @Test
    public void urlPattern() {
        Matcher matcher = MySQLUrlParser.CONNECTION_STRING_PTRN.matcher("jdbc:mysql:///army");
        if (!matcher.matches()) {
            return;
        }
        LOG.info("authority:{}", matcher.group("authority"));
    }

    @Test
    public void isAddressEqualsHostPrefix() {
        String authority = "address = ( host=kafka)(port=3435)(key2=value2)";
        if (MySQLUrlParser.isAddressEqualsHostPrefix(authority, 0)) {
            LOG.info("\"{}\" is address-equals host", authority);
        }

    }


    @Test(invocationCount = 100)
    public void pattern() {
        long start = System.currentTimeMillis();
        String url = "jdbc:mysql://address=(host=kafka)(port=3435)(key2=value2),localhost,( host  =  kosmo , port = 3306 ),( host  =  simonyi , port = 9987 ),zoro:3306,address=(host=myhost2)(port=2222)(key2=value2)/db?useSSL=true";
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "dfsd");
        ConnectionUrl.getConnectionUrlInstance(url, properties);
        LOG.info("cost {} ms", System.currentTimeMillis() - start);
    }


    @Test
    public void mysqlConnect() throws Exception {
        String url = "jdbc:mysql://(address=(host=localhost)(port=3306))/army?useSSL=false&detectCustomCollations=true";
        Driver.class.getName();
        try (Connection conn = DriverManager.getConnection(url, "army_w", "army123")) {
            try (Statement st = conn.createStatement()) {
                try (ResultSet resultSet = st.executeQuery("SELECT NOW()")) {
                    if (resultSet.next()) {
                        LOG.info("now:{}", resultSet.getString(1));
                    }
                }
            }
        }
    }

    @Test
    public void preparedStatement() throws Exception {
        String url = "jdbc:mysql://(address=(host=localhost)(port=3306))/army?useSSL=false&detectCustomCollations=true&useServerPrepStmts=true";
        Driver.class.getName();
        try (Connection conn = DriverManager.getConnection(url, "army_w", "army123")) {
            try (PreparedStatement st = conn.prepareStatement("SELECT u.id,u.nick_name FROM u_user AS u WHERE u.id = ?")) {
                st.setLong(1, 1L);
                try (ResultSet resultSet = st.executeQuery()) {
                    while (resultSet.next()) {
                        LOG.info("id:{}\nnickName:{}\n\n\n", resultSet.getLong(1), resultSet.getString(2));
                    }
                }
            }
        }
    }

    @Test
    public void loadLocalInfile() throws Exception {
        String url = "jdbc:mysql://(address=(host=localhost)(port=3306))/army?allowLoadLocalInfile=true&sslMode=DISABLED&detectCustomCollations=true&useServerPrepStmts=true";
        Driver.class.getName();
        String sql = "LOAD DATA LOCAL INFILE '/Users/zoro/repository/my-github/idea/jdbd/jdbd-mysql/src/test/resources/my-local/zoro_user.sql'\n" +
                " INTO TABLE u_user;";
        try (Connection conn = DriverManager.getConnection(url, "army_w", "army123")) {
            try (PreparedStatement st = conn.prepareStatement(sql)) {
                LOG.info("update rows:{}", st.executeUpdate());
            }
        }
    }

}
