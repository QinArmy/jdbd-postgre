package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.postgre.ClientTestUtils;
import io.jdbd.postgre.Group;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

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
@Test(groups = {Group.COPY_IN_OPERATION}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS, Group.SESSION_BUILDER
        , Group.TASK_TEST_ADVICE, Group.SIMPLE_QUERY_TASK, Group.EXTENDED_QUERY_TASK})
public class CopyInSuiteTests extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(CopyInSuiteTests.class);

    private static final String DATA_DIR = ClientTestUtils.getTestResourcesPath().toString();

    private static final String LINE_SEPARATOR = System.lineSeparator();


    @Test
    public void simpleQueryCopyInFromLocalFileWithStatic() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final Path path = Paths.get(DATA_DIR, "data/copy/my_copies.csv").toAbsolutePath();

        final String sql = String.format(
                "/* comment */ COPY my_copies(create_time,my_varchar) FROM --comment%s'%s'  WITH CSV"
                , LINE_SEPARATOR, path);

        final ResultStates state;
        state = SimpleQueryTask.update(PgStmts.stmt(sql), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        assertNotNull(state, "state");
        assertTrue(state.getAffectedRows() > 0L, "affectedRows");
        assertFalse(state.hasMoreFetch(), "more fetch");
        assertFalse(state.hasColumn(), "hasColumn");
        assertFalse(state.hasMoreResult(), "more result");

    }

    @Test
    public void simpleQueryCopyInFromLocalFileWithGroup() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final Path path = Paths.get(DATA_DIR, "data/copy/my_copies.csv").toAbsolutePath();


        final String sql = String.format(
                "/* comment */ COPY my_copies(create_time,my_varchar) FROM --comment%s'%s'  WITH CSV"
                , LINE_SEPARATOR, path);

        final List<String> sqlGroup = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            sqlGroup.add(sql);
        }

        final List<ResultStates> stateList;
        stateList = SimpleQueryTask.batchUpdate(PgStmts.group(sqlGroup), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        assertNotNull(stateList, "stateList");
        assertEquals(stateList.size(), sqlGroup.size(), "stateList size");

        int count = 0;
        for (ResultStates state : stateList) {
            assertTrue(state.getAffectedRows() > 0L, "affectedRows");
            assertFalse(state.hasMoreFetch(), "more fetch");
            assertFalse(state.hasColumn(), "hasColumn");

            if (count == stateList.size() - 1) {
                assertFalse(state.hasMoreResult(), "more result");
            } else {
                assertTrue(state.hasMoreResult(), "more result");
            }

            count++;
        }

    }


    @Test
    public void simpleQueryCopyInFromLocalFileWithBindable() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final Path path = Paths.get(DATA_DIR, "data/copy/my_copies.csv").toAbsolutePath();

        final String sql = String.format(
                "/* comment */ COPY my_copies(create_time,my_varchar) FROM --comment%s?  WITH CSV"
                , LINE_SEPARATOR);

        final ResultStates state;
        state = SimpleQueryTask.bindableUpdate(PgStmts.bind(sql, BindValue.wrap(0, PgType.VARCHAR, path.toString())), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        assertNotNull(state, "state");
        assertTrue(state.getAffectedRows() > 0L, "affectedRows");
        assertFalse(state.hasMoreFetch(), "more fetch");
        assertFalse(state.hasColumn(), "hasColumn");
        assertFalse(state.hasMoreResult(), "more result");

    }

    @Test
    public void simpleQueryCopyInFromLocalFileWithBatchBindable() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final String fileName = Paths.get(DATA_DIR, "data/copy/my_copies.csv").toAbsolutePath().toString();

        final String sql = String.format(
                "/* comment */ COPY my_copies(create_time,my_varchar) FROM --comment%s?  WITH CSV"
                , LINE_SEPARATOR);

        final List<List<BindValue>> groupList = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            groupList.add(Collections.singletonList(BindValue.wrap(0, PgType.VARCHAR, fileName)));
        }

        final List<ResultStates> stateList;
        stateList = SimpleQueryTask.bindableBatchUpdate(PgStmts.bindableBatch(sql, groupList), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        assertNotNull(stateList, "stateList");
        assertEquals(stateList.size(), groupList.size(), "stateList size");
        int count = 0;
        for (ResultStates state : stateList) {

            assertTrue(state.getAffectedRows() > 0L, "affectedRows");
            assertFalse(state.hasMoreFetch(), "more fetch");
            assertFalse(state.hasColumn(), "hasColumn");

            if (count == stateList.size() - 1) {
                assertFalse(state.hasMoreResult(), "more result");
            } else {
                assertTrue(state.hasMoreResult(), "more result");
            }

            count++;
        }

    }

    @Test
    public void simpleQueryCopyInFromLocalFileWithMultiBindStmt() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final String fileName = Paths.get(DATA_DIR, "data/copy/my_copies.csv").toAbsolutePath().toString();

        final String sql = String.format(
                "/* comment */ COPY my_copies(create_time,my_varchar) FROM --comment%s?  WITH CSV"
                , LINE_SEPARATOR);

        final List<BindStmt> stmtGroup = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            BindStmt stmt = PgStmts.bind(sql, Collections.singletonList(BindValue.wrap(0, PgType.VARCHAR, fileName)));
            stmtGroup.add(stmt);
        }

        final List<ResultStates> stateList;
        stateList = SimpleQueryTask.multiStmtAsFlux(PgStmts.multi(stmtGroup), adjutant)
                .cast(ResultStates.class)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        assertNotNull(stateList, "stateList");
        assertEquals(stateList.size(), stmtGroup.size(), "stateList size");
        int count = 0;
        for (ResultStates state : stateList) {

            assertTrue(state.getAffectedRows() > 0L, "affectedRows");
            assertFalse(state.hasMoreFetch(), "more fetch");
            assertFalse(state.hasColumn(), "hasColumn");

            if (count == stateList.size() - 1) {
                assertFalse(state.hasMoreResult(), "more result");
            } else {
                assertTrue(state.hasMoreResult(), "more result");
            }

            count++;
        }

    }


    @Test(expectedExceptions = JdbdSQLException.class)
    public void simpleQueryCopyInFromProgramMode() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        final String sql = String.format(
                "/* comment */ COPY my_copies(create_time,my_varchar) FROM --comment%sPROGRAM 'FAKE COMMAND'  WITH CSV"
                , LINE_SEPARATOR);

        SimpleQueryTask.update(PgStmts.stmt(sql), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

    }

    @Test(expectedExceptions = JdbdSQLException.class)
    public void simpleQueryCopyInFromStdin() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        final String sql = String.format(
                "/* comment */ COPY my_copies(create_time,my_varchar) FROM --comment%sSTDIN  WITH CSV"
                , LINE_SEPARATOR);

        SimpleQueryTask.update(PgStmts.stmt(sql), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

    }


    @SuppressWarnings("all")
    private static final class CopyInDataPublisher implements Publisher<byte[]> {

        private final Path path;

        private CopyInDataPublisher(Path path) {
            this.path = path;
        }

        @Override
        public final void subscribe(Subscriber<? super byte[]> s) {
            s.onSubscribe(new CopyInDataSubscription(path, s));
        }


    }

    private static final class CopyInDataSubscription implements Subscription {

        private static final AtomicIntegerFieldUpdater<CopyInDataSubscription> STATE = AtomicIntegerFieldUpdater
                .newUpdater(CopyInDataSubscription.class, "state");

        private final Path path;

        private final Subscriber<? super byte[]> subscriber;

        /**
         * <ul>
         *     <li>0: init</li>
         *     <li>1: coping data</li>
         *     <li>2: cancel</li>
         *     <li>3: complete </li>
         *     <li>4: error</li>
         * </ul>
         */
        private volatile int state = 0;

        private CopyInDataSubscription(Path path, Subscriber<? super byte[]> subscriber) {
            this.path = path;
            this.subscriber = subscriber;
        }

        @Override
        public final void request(long n) {
            //no-ope
            if (STATE.compareAndSet(this, 0, 1)) {
                sendCopyInData();
            }
        }

        @Override
        public final void cancel() {
            STATE.compareAndSet(this, 1, 2);
        }

        private void sendCopyInData() {
            final Subscriber<? super byte[]> subscriber = this.subscriber;

            try (FileChannel channel = FileChannel.open(this.path, StandardOpenOption.READ)) {
                final byte[] bufferArray = new byte[(int) Math.min(2048, channel.size())];
                final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

                while (channel.read(buffer) > 0) {
                    buffer.flip();
                    if (buffer.remaining() == bufferArray.length) {
                        subscriber.onNext(Arrays.copyOf(bufferArray, bufferArray.length));
                    } else {
                        subscriber.onNext(Arrays.copyOfRange(bufferArray, 0, buffer.limit()));
                    }
                    buffer.clear();
                    if (this.state == 2) {
                        break;
                    }
                }

                STATE.compareAndSet(this, 1, 3);
                subscriber.onComplete();
            } catch (Throwable e) {
                LOG.error("Copy in data occur error.", e);
                STATE.set(this, 4);
                subscriber.onError(e);
            }

        }


    }


}
