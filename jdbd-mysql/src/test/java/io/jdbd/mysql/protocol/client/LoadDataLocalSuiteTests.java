package io.jdbd.mysql.protocol.client;


import io.jdbd.mysql.Groups;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.stmt.BindableStmt;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;

import static org.testng.Assert.*;

/**
 * <p>
 * This class test LOAD DATA LOCAL statement.
 * </p>
 *
 * @see ComQueryTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_local_infile_request.html">Protocol::LOCAL INFILE Request</a>
 */
@Test(groups = {Groups.LOAD_DATA}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS
        , Groups.COM_QUERY_WRITER})
public class LoadDataLocalSuiteTests extends AbstractStmtTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(LoadDataLocalSuiteTests.class);

    public LoadDataLocalSuiteTests() {
        super(SubType.COM_QUERY);
    }

    @Override
    Mono<ResultStatus> executeUpdate(BindableStmt stmt, MySQLTaskAdjutant adjutant) {
        return ComQueryTask.bindableUpdate(stmt, adjutant);
    }

    @Override
    Flux<ResultRow> executeQuery(BindableStmt stmt, MySQLTaskAdjutant adjutant) {
        return ComQueryTask.bindableQuery(stmt, adjutant);
    }

    @Override
    Logger obtainLogger() {
        return LOG;
    }

    @BeforeClass(timeOut = TIME_OUT)
    public static void beforeClass() {
        LOG.info("{} beforeClass test start", Groups.LOAD_DATA);

        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        final String ddl = "CREATE TABLE IF NOT EXISTS mysql_load_data(\n" +
                "    id bigint AUTO_INCREMENT,\n" +
                "    create_time datetime NOT NULL  DEFAULT current_timestamp,\n" +
                "    name VARCHAR(30) NOT  NULL DEFAULT '',\n" +
                "    PRIMARY KEY (id)\n" +
                ")";

        LOG.info("{} ddl:\n:{}", Groups.LOAD_DATA, ddl);

        ComQueryTask.update(Stmts.stmt(ddl), adjutant)
                .doOnNext(Assert::assertNotNull)
                .then(ComQueryTask.update(Stmts.stmt("TRUNCATE mysql_load_data"), adjutant))
                .doOnNext(Assert::assertNotNull)
                .then()
                .block();

        releaseConnection(adjutant);
        LOG.info("{} beforeClass test success", Groups.LOAD_DATA);
    }

    @AfterClass(timeOut = TIME_OUT)
    public static void afterClass() {
        LOG.info("{} afterClass test start", Groups.LOAD_DATA);

        if (ClientTestUtils.getTestConfig().getProperty("truncate.after.suite", Boolean.class, Boolean.TRUE)) {

            final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
            final String sql = "TRUNCATE mysql_load_data";

            ResultStatus status;
            status = ComQueryTask.update(Stmts.stmt(sql), adjutant)
                    .block();

            assertNotNull(status, sql);

            releaseConnection(adjutant);
        }

        LOG.info("{} afterClass test success", Groups.LOAD_DATA);

    }


    @Test(timeOut = TIME_OUT)
    public void loadDataLocal() throws IOException {
        LOG.info("loadDataLocal test start");

        final int rowCount = 5000;
        final Path path;
        path = createDataFile("mysql_load_data", rowCount);
        try {
            doLoadData(path, rowCount);
        } finally {
            Files.deleteIfExists(path);
        }
        LOG.info("loadDataLocal test success");
    }

    @Test(timeOut = 10L * 1000L)
    public void loadDataLocal22m() throws IOException {
        LOG.info("loadDataLocal22m test start");

        final int rowCount = 600000;
        final Path path;
        path = createDataFile("mysql_load_data_22m", rowCount);

        try {
            doLoadData(path, rowCount);
        } finally {
            Files.deleteIfExists(path);
        }
        LOG.info("loadDataLocal22m test success");
    }




    /*################################## blow private method ##################################*/

    private Path createDataFile(String prefix, int rowCount) throws IOException {
        final Path path = Files.createTempFile(prefix, ".csv");

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            final String now = LocalDateTime.now().format(MySQLTimeUtils.MYSQL_DATETIME_FORMATTER_0);
            StringBuilder builder = new StringBuilder(rowCount * 25);
            for (int i = 0; i < rowCount; i++) {
                if (i > 0) {
                    builder.append("\n");
                }
                builder.append(now)
                        .append("\t")
                        .append("army")
                        .append(i);
            }
            byte[] bytes = builder.toString().getBytes(StandardCharsets.UTF_8);
            channel.write(ByteBuffer.wrap(bytes));
            return path;
        } catch (Throwable e) {
            Files.deleteIfExists(path);
            throw e;
        }
    }

    private void doLoadData(Path path, long rows) {
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        if (!adjutant.obtainHostInfo().getProperties().getOrDefault(PropertyKey.allowLoadLocalInfile, Boolean.class)) {
            fail(String.format("client no support Load data local statement,please config property[%s]"
                    , PropertyKey.allowLoadLocalInfile));
        }
        if (!adjutant.obtainServer().supportLocalInfile()) {
            LOG.warn("Server no support Local infile ,please config system variables[@@GLOBAL.local_infile]");
            return;
        }


        final String sql = String.format("LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE mysql_load_data (create_time,name)"
                , path.toAbsolutePath());

        LOG.info("execute loadDataLocal sql :{} ", sql);
        ResultStatus status;
        status = ComQueryTask.update(Stmts.stmt(sql), adjutant)
                .block();

        assertNotNull(status, sql);
        LOG.info("Local file:{} affectedRows:{}", path, status.getAffectedRows());
        assertEquals(status.getAffectedRows(), rows, sql);

        releaseConnection(adjutant);
    }


}
