package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Groups;
import io.jdbd.mysql.util.MySQLNumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import static org.testng.Assert.assertEquals;

/**
 * This class test query big column ,eg: {@link io.jdbd.mysql.MySQLType#LONGBLOB}
 *
 * @see ComQueryTask
 * @see TextResultSetReader
 */
@Test(groups = {Groups.TEXT_RESULT_SET_READER}, dependsOnGroups = {Groups.COM_QUERY, Groups.DATA_PREPARE})
public class ComQueryTaskBigColumnSuiteTests extends AbstractConnectionBasedSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTaskBigColumnSuiteTests.class);

    @Test(enabled = false)
    public void longBlob() throws Exception {
        LOG.info("longBlob test start");
        final Path longBlobFile;
        longBlobFile = crateLongBlobFile();

        Files.deleteIfExists(longBlobFile);
        LOG.info("longBlob test end");
    }


    private Path crateLongBlobFile() throws Exception {
        final Random random = new Random();
        final byte[] blockArray = new byte[1024];
        for (int i = 0, offset = 0; i < 128; i++) {
            MySQLNumberUtils.longToBigEndian(random.nextLong(), blockArray, offset, 8);
            offset += 8;
        }
        final Path dir = ClientTestUtils.getBigColumnTestPath();
        if (Files.notExists(dir)) {
            Files.createDirectories(dir);
        }
        final Path longBlobFile = Files.createTempFile(dir, "longBlob", ".b");

        try (FileChannel channel = FileChannel.open(longBlobFile, StandardOpenOption.WRITE)) {
            final ByteBuffer buffer = ByteBuffer.wrap(blockArray);
            buffer.position(buffer.limit());
            final int end = 1 << 22;
            for (int i = 1; i <= end; i++) {
                buffer.flip();
                if (i == end) {
                    buffer.get();
                }
                channel.write(buffer);
                buffer.clear();
                buffer.position(buffer.limit());
            }
            assertEquals(channel.size(), (1L << 32) - 1L, "size");
            return longBlobFile;
        } catch (Throwable e) {
            Files.deleteIfExists(longBlobFile);
            throw e;
        }


    }


}