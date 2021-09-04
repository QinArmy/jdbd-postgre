package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.ClientTestUtils;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

public class JdbcUnitTests {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUnitTests.class);


    private static final String url = "jdbc:postgresql://localhost:5432/army_test?options=-c%20IntervalStyle=iso_8601&currentSchema=army";
    private ByteBuffer buffer;

    @Test
    public void connect() throws Exception {

        try (Connection conn = DriverManager.getConnection(url, createProperties())) {
            String sql = "CALL my_update_procedure(1);";
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(sql);
            }
        }

    }


    @Test
    public void prepare() throws Exception {
        Properties properties = new Properties(createProperties());
        properties.put("preferQueryMode", "extended");

        try (Connection conn = DriverManager.getConnection(url, properties)) {
            String sql = "UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, "4567898765456789876545678987656789876");
                stmt.addBatch();
                stmt.setString(1, "4567898765456789876545678987656789876");
                stmt.addBatch();

                stmt.executeBatch();

            }
        }

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

        properties.put("currentSchema", "army");
        return properties;
    }


}
