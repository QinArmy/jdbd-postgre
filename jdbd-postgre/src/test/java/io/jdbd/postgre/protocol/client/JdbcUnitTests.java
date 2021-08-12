package io.jdbd.postgre.protocol.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

public class JdbcUnitTests {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUnitTests.class);


    private static final String url = "jdbc:postgresql://localhost:5432/army_test?options=-c%20IntervalStyle=iso_8601";
    private ByteBuffer buffer;

    @Test
    public void connect() throws Exception {

        try (Connection conn = DriverManager.getConnection(url, createProperties())) {
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


    @Test
    public void createJson() throws Exception {

    }


    @Test
    public void bigColumn() throws Exception {
        final Path path = Paths.get(ClientTestUtils.getClassPath().toString(), "myLongJson.json");
        createBigJsonFile(path);

        try (Connection conn = DriverManager.getConnection(url, createProperties())) {
            try (PreparedStatement ps = conn.prepareStatement("UPDATE mysql_types AS t SET my_json = ? WHERE t.id = 1")) {
                try (InputStream in = Files.newInputStream(path, StandardOpenOption.READ)) {
                    ps.setBinaryStream(1, in);

                    System.out.println(ps.executeUpdate());
                }
            }
        } finally {
            Files.deleteIfExists(path);
        }
    }


    private void createBigJsonFile(Path path) throws Exception {
        final StandardOpenOption[] options = new StandardOpenOption[]{
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        };
        try (FileChannel channel = FileChannel.open(path, options)) {
            final String map = "{\"id\":33,\"name\":\"Army\"}";
            final byte[] array = map.getBytes(StandardCharsets.US_ASCII);
            final byte[] oneGit = Arrays.copyOf(array, 1 << 30);
            for (int i = array.length - 2, end = oneGit.length - 2; i < end; i++) {
                oneGit[i] = 'A';
            }
            oneGit[oneGit.length - 2] = '\"';
            oneGit[oneGit.length - 1] = '}';

            final ByteBuffer buf = ByteBuffer.allocate(1);
            buf.put((byte) '[');
            buf.flip();
            channel.write(buf);
            buf.clear();

            final ByteBuffer buffer = ByteBuffer.wrap(oneGit);
            channel.write(buffer);
            buffer.clear();
            buf.put((byte) ',');

            for (int i = 0, end = 7; i < end; i++) {
                buf.flip();
                channel.write(buf);
                channel.write(buffer);
                buffer.clear();
            }

            buf.clear();
            buf.put((byte) ']');
            buf.flip();
            channel.write(buf);
            buf.clear();
            long size = channel.size();

            System.out.println(size >> 30);
        }
    }


    private Properties createProperties() {
        Properties properties = new Properties();
        properties.put("user", "army_w");
        properties.put("password", "army123");
        properties.put("preferQueryMode", "simple");
        properties.put("currentSchema", "army");
        return properties;
    }


}
