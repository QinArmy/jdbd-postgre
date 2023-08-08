package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.ClientTestUtils;
import io.jdbd.postgre.Group;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.testng.Assert.*;

/**
 * <p>
 * This class is test class of below:
 * <ul>
 *     <li>{@link SimpleQueryTask}</li>
 *     <li>{@link ExtendedQueryTask}</li>
 * </ul>
 * </p>
 *
 * @see SimpleQueryTask
 */
@Test(groups = {Group.COPY_OUT_OPERATION}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS, Group.SESSION_BUILDER
        , Group.TASK_TEST_ADVICE, Group.SIMPLE_QUERY_TASK, Group.EXTENDED_QUERY_TASK, Group.COPY_IN_OPERATION})
public class CopyOutSuiteTests extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(CopyOutSuiteTests.class);

    private static final String CLASS_PATH = ClientTestUtils.getClassPath().toString();

    private static final String LINE_SEPARATOR = System.lineSeparator();


    /**
     * need config Permission
     */
    @Test(enabled = false)
    public void copyOutToFileWithStatic() throws IOException {
        // TODO config Permission test
        final PgProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final Path path = Paths.get(CLASS_PATH, "my_copies_output_with_static.csv").toAbsolutePath();
        final String sql = String.format(
                "/* comment */ COPY my_copies(id,create_time,my_varchar) TO --comment%s'%s'  WITH CSV"
                , LINE_SEPARATOR, path);

        final ResultStates state;
        state = SimpleQueryTask.update(PgStmts.stmt(sql), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        assertNotNull(state, "state");
        assertEquals(state.affectedRows(), 0L, "affectedRows");
        assertFalse(state.hasMoreFetch(), "more fetch");
        assertFalse(state.hasColumn(), "hasColumn");
        assertFalse(state.hasMoreResult(), "more result");

        assertTrue(Files.size(path) > 0, "path size");

    }

    @Test
    public void copyOutToStdoutWithStatic() {
        final PgProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final String sql = String.format(
                "/* comment */ COPY my_copies(id,create_time,my_varchar) TO --comment%sSTDOUT  WITH CSV"
                , LINE_SEPARATOR);

        final ResultStates state;
        state = SimpleQueryTask.update(PgStmts.stmtWithExport(sql, StdoutSubscriber::create), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        assertNotNull(state, "state");
        assertTrue(state.affectedRows() > 0L, "affectedRows");
        assertFalse(state.hasMoreFetch(), "more fetch");
        assertFalse(state.hasColumn(), "hasColumn");
        assertFalse(state.hasMoreResult(), "more result");

    }


    private static final class StdoutSubscriber implements Subscriber<byte[]> {

        private static StdoutSubscriber create(Object obj) {
            return new StdoutSubscriber((Charset) obj);
        }

        private final Charset charset;

        private StdoutSubscriber(Charset charset) {
            this.charset = charset;
        }

        @Override
        public final void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public final void onNext(byte[] bytes) {
            System.out.print(new String(bytes, charset));
        }

        @Override
        public final void onError(Throwable t) {
            System.out.println(t);
        }

        @Override
        public final void onComplete() {
            //no-ope
        }


    }


}
