package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLStates;
import io.jdbd.vendor.util.SQLStates;
import io.netty.buffer.ByteBuf;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.netty.Connection;
import reactor.netty.tcp.SslProvider;
import reactor.netty.tcp.TcpClient;

import javax.net.ssl.SSLException;
import java.io.BufferedReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SslTest {
    private static final Logger LOG = LoggerFactory.getLogger(SslTest.class);


    @Test
    public void sslTest() {

        TcpClient.create()
                //.runOn(eventLoopGroup)
                .host("localhost")
                .port(3306)
                .secure(this::configSSlContext)
                .connect()
                .map(this::task)
                .block()
        ;
    }

    @Test
    public void sslEngine() throws Exception {
        Path workDir = Paths.get(System.getProperty("user.dir"));
        Path projectDir = workDir.getParent();
        Path path = Paths.get(projectDir.toString(), "/doc/vendor/db2/SQLStates.txt");
        Map<String, String> db2Map = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            //  int count = 0;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("//")) {
                    // System.out.println();
                    //  System.out.println(line);
                    // count = 0;
                    continue;
                }
                int index = line.indexOf('\t');
                if (index < 0) {
                    throw new RuntimeException(line);
                }
                db2Map.put(line.substring(0, index), line.substring(index));
            }

        }
        Map<String, String> stateMap = new HashMap<>();
        Field[] fields = SQLStates.class.getDeclaredFields();

        for (Field field : fields) {
            stateMap.put((String) field.get(null), field.getName());
        }

        for (Map.Entry<String, String> e : db2Map.entrySet()) {
            System.out.printf("%s : %s \n", e.getKey(), e.getValue());
        }
        System.out.printf("db2Map size:%s\n", db2Map.size());
        System.out.printf("stateMap size:%s\n", stateMap.size());

        Set<String> keySet = new HashSet<>(db2Map.keySet());

        keySet.retainAll(stateMap.keySet());

        System.out.printf("retainAll size:%s\n", keySet.size());
        for (String code : keySet) {
            System.out.println(code);
        }

    }

    @Test
    public void simple() throws Exception {
        final Field[] statesFields = SQLStates.class.getDeclaredFields();
        Map<String, String> stateMap = new HashMap<>();
        for (Field field : statesFields) {
            stateMap.put((String) field.get(null), field.getName());
        }


        final Field[] mysqlStatesFields = MySQLStates.class.getDeclaredFields();
        Map<String, String> mysqlStatsMap = new HashMap<>();
        for (Field field : mysqlStatesFields) {
            mysqlStatsMap.put((String) field.get(null), field.getName());
        }

        Set<String> stateCodeSet = new HashSet<>(mysqlStatsMap.keySet());
        stateCodeSet.retainAll(stateMap.keySet());

        for (Field field : mysqlStatesFields) {
            System.out.printf("map.put();");
        }
    }

    @Test
    public void simpleTest() throws Exception {

    }

    @Test
    public void cipherSuits() throws Exception {


    }

    private void configSSlContext(SslProvider.SslContextSpec spec) {
        try {
            SslContext context = SslContextBuilder.forClient()

                    .startTls(true)
                    .build();
            spec.sslContext(context);
        } catch (SSLException e) {
            e.printStackTrace();
        }

    }

    private Connection task(Connection connection) {
        connection.inbound().receive()
                .map(this::readHandshake)
                .subscribe()
        ;

        return connection;
    }

    HandshakeV10Packet readHandshake(ByteBuf packet) {
        int payloadLength = PacketUtils.readInt3(packet);
        LOG.debug("handshake sequenceId:{}", PacketUtils.readInt1(packet));
        HandshakeV10Packet handshakeV10Packet = HandshakeV10Packet.readHandshake(packet.readSlice(payloadLength));
        LOG.debug("handshakeV10Packet:\n{}", handshakeV10Packet);
        return handshakeV10Packet;
    }
}
