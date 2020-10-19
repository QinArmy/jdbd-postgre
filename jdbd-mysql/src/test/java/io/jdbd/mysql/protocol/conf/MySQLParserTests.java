package io.jdbd.mysql.protocol.conf;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.jdbc.Driver;
import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.mysql.protocol.client.ClientProtocolImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MySQLParserTests {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLParserTests.class);


    @Test(invocationCount = 100)
    public void simpleTest() throws Exception {
        long start = System.currentTimeMillis();
        String url = "jdbc:mysql://address=(host=kafka)(port=3435)(key2=value2),localhost,( host  =  kosmo , port = 3306 ),( host  =  simonyi , port = 9987 ),zoro:3306,address=(host=myhost2)(port=2222)(key2=value2)/db?useSSL=true";
        MySQLUrlParser parser = MySQLUrlParser.parseConnectionString(url, Collections.singletonMap("user", "army"));
        LOG.info("cost {} ms", System.currentTimeMillis() - start);
        // LOG.info("protocol:{}", parser.getScheme());
        //  LOG.info("authority:{}", parser.getAuthority());
//        int index = 0;
//        for (HostInfo host : parser.getParsedHosts()) {
//            LOG.info("host-{}:{},port:{}", index, host.getHost(), host.getPort());
//            index++;
//        }

        //LOG.info("path:{}", parser.getPath());
        // LOG.info("query:{}", parser.getQuery());

    }

    @Test
    public void handshake()throws Exception {
        String url = "jdbc:mysql://localhost:3306/army";
        Map<String, String> properties = new HashMap<>();
        properties.put("user", "army_w");
        properties.put("password", "army123");
        MySQLPacket mySQLPacket = ClientProtocolImpl.getInstance(MySQLUrl.getInstance(url, properties))
                .flatMap(ClientProtocol::handshake)
                .block();
        LOG.info("handshake packet:\n {}", mySQLPacket);
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
        String url = "jdbc:mysql://(address=(host=localhost)(port=3306))/army";
        Driver.class.getName();
        try (Connection conn = DriverManager.getConnection(url, "army_w", "army123")) {
            try (Statement st = conn.createStatement()) {
                try (ResultSet resultSet = st.executeQuery("SELECT now()")) {
                    if (resultSet.next()) {
                        LOG.info("now:{}", resultSet.getString(1));
                    }
                }
            }
        }
    }

}
