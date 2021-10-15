package io.jdbd.vendor.stmt;


import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.util.JdbdCollections;
import io.jdbd.vendor.util.JdbdFunctions;
import io.jdbd.vendor.util.JdbdStrings;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class JdbdStmts {

    protected JdbdStmts() {
        throw new UnsupportedOperationException();
    }


    public static ParamBatchStmt<ParamValue> batch(String sql, List<List<ParamValue>> groupList) {
        if (groupList.size() < 1) {
            throw new IllegalArgumentException("groupList is empty.");
        }
        return new BatchStmtImpl(sql, groupList, 0);
    }

    public static ParamBatchStmt<ParamValue> batch(String sql, List<List<ParamValue>> groupList, int timeOut) {
        if (groupList.size() < 2) {
            throw new IllegalArgumentException("groupList size < 2");
        }
        return new BatchStmtImpl(sql, groupList, timeOut);
    }

    public static StaticStmt stmt(String sql) {
        Objects.requireNonNull(sql, "sql");
        return new StmtImpl1(sql);
    }

    public static StaticStmt stmt(String sql, StatementOption option) {
        return new StaticStmtFull(sql, JdbdFunctions.noActionConsumer(), option);
    }


    public static StaticStmt stmt(String sql, Consumer<ResultStates> statesConsumer, StatementOption option) {
        return new StaticStmtFull(sql, statesConsumer, option);
    }

    public static StaticStmt stmtWithExport(String sql, Function<Object, Subscriber<byte[]>> function) {
        return new StaticStmtWithExport(sql, 0, function);
    }

    public static StaticStmt stmt(String sql, int timeout) {
        Objects.requireNonNull(sql, "sql");
        return timeout > 0 ? new StmtImpl2(sql, timeout) : new StmtImpl1(sql);
    }

    public static StaticStmt stmt(String sql, Consumer<ResultStates> statusConsumer) {
        Objects.requireNonNull(sql, "sql");
        return new StmtImpl2C(sql, statusConsumer);
    }

    public static StaticStmt stmt(String sql, Consumer<ResultStates> statusConsumer, int timeout) {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(statusConsumer, "statusConsumer");
        return timeout > 0
                ? new StmtImpl3(sql, timeout, statusConsumer)
                : new StmtImpl2C(sql, statusConsumer);
    }

    public static List<StaticStmt> stmts(List<String> sqlList) {
        return stmts(sqlList, 0);
    }

    public static List<StaticStmt> stmts(List<String> sqlList, int timeout) {
        Objects.requireNonNull(sqlList, "sqlList");
        if (sqlList.isEmpty()) {
            throw new IllegalArgumentException("sqlList is empty.");
        }
        final List<StaticStmt> list;
        final int size = sqlList.size();
        if (size == 1) {
            list = Collections.singletonList(stmt(sqlList.get(0), timeout));
        } else {
            List<StaticStmt> tempList = new ArrayList<>(size);
            for (String sql : sqlList) {
                tempList.add(stmt(sql, timeout));
            }
            list = Collections.unmodifiableList(tempList);
        }
        return list;
    }

    public static StaticBatchStmt group(List<String> sqlGroup, int timeout) {
        return timeout == 0 ? new GroupStmtZeroTimeout(sqlGroup) : new GroupStmtImpl(sqlGroup, timeout);
    }

    public static StaticBatchStmt group(List<String> sqlGroup) {
        return new GroupStmtZeroTimeout(sqlGroup);
    }

    public static StaticBatchStmt batchStmt(final List<String> sqlGroup, final StatementOption option) {
        final List<String> list;
        switch (sqlGroup.size()) {
            case 0:
                throw new IllegalArgumentException("Empty sqlGroup");
            case 1:
                list = Collections.singletonList(sqlGroup.get(0));
                break;
            default: {
                final List<String> tempList = new ArrayList<>(sqlGroup.size());

                for (String sql : sqlGroup) {
                    if (!JdbdStrings.hasText(sql)) {
                        throw new IllegalArgumentException("Each sql must have text.");
                    }
                    tempList.add(sql);
                }
                list = Collections.unmodifiableList(tempList);
            }
        }
        return new BatchStmtFull(list, option);
    }


    public static ParamStmt multiPrepare(String sql, List<? extends ParamValue> paramGroup) {
        return new SimpleParamStmt(sql, paramGroup);
    }

    public static ParamStmt paramStmt(String sql, List<? extends ParamValue> paramGroup) {
        return new ParamStmtMin(sql, paramGroup);
    }

    public static ParamStmt paramStmt(String sql, List<? extends ParamValue> paramGroup, StatementOption option) {
        return new ParamStmtFull(sql, paramGroup, JdbdFunctions.noActionConsumer(), option);
    }

    public static ParamStmt paramStmt(String sql, List<? extends ParamValue> paramGroup
            , Consumer<ResultStates> statesConsumer, StatementOption option) {
        return new ParamStmtFull(sql, paramGroup, statesConsumer, option);
    }

    public static ParamBatchStmt<ParamValue> paramBatch(String sql, List<List<ParamValue>> groupList) {
        return new ParamBatchStmtMin(sql, groupList);
    }

    public static ParamBatchStmt<ParamValue> paramBatch(String sql, List<List<ParamValue>> groupList
            , StatementOption option) {
        return new ParamBatchStmtFull(sql, groupList, option);
    }

    protected static List<String> wrapSqlGroup(final List<String> sqlGroup) {
        Objects.requireNonNull(sqlGroup, "sqlGroup");
        final List<String> newSqlGroup;
        switch (sqlGroup.size()) {
            case 0:
                throw new IllegalArgumentException("sqlGroup must not empty.");
            case 1: {
                final String sql = sqlGroup.get(0);
                if (sql == null) {
                    throw new NullPointerException("sql as index[0] is null");
                }
                newSqlGroup = Collections.singletonList(sql);
            }
            break;
            default: {
                final List<String> list = new ArrayList<>(sqlGroup.size());
                final int size = sqlGroup.size();
                String sql;
                for (int i = 0; i < size; i++) {
                    sql = sqlGroup.get(i);
                    if (sql == null) {
                        throw new NullPointerException(String.format("sql at index[%s] has no text.", i));
                    }
                    list.add(sql);
                }
                newSqlGroup = Collections.unmodifiableList(list);
            }
        }
        return newSqlGroup;
    }


    protected static abstract class AbstractMinStmt implements Stmt {

        @Override
        public final int getFetchSize() {
            return 0;
        }

        @Override
        public final Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public final Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }

    }

    private static final class ParamBatchStmtFull implements ParamBatchStmt<ParamValue> {

        private final String sql;

        private final List<List<ParamValue>> groupList;

        private final int timeout;

        private final int fetchSize;

        private final Function<Object, Publisher<byte[]>> importPublisher;

        private final Function<Object, Subscriber<byte[]>> exportSubscriber;


        private ParamBatchStmtFull(String sql, List<List<ParamValue>> groupList, StatementOption option) {
            this.sql = sql;
            this.groupList = JdbdCollections.unmodifiableList(groupList);
            this.timeout = option.getTimeout();

            this.fetchSize = option.getFetchSize();
            this.importPublisher = option.getImportPublisher();
            this.exportSubscriber = option.getExportSubscriber();

        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<List<ParamValue>> getGroupList() {
            return this.groupList;
        }

        @Override
        public int getTimeout() {
            return this.timeout;
        }

        @Override
        public int getFetchSize() {
            return this.groupList.size() == 1 ? this.fetchSize : 0;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return this.groupList.size() == 1 ? this.importPublisher : null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return this.groupList.size() == 1 ? this.exportSubscriber : null;
        }

    }

    private static final class ParamBatchStmtMin extends AbstractMinStmt implements ParamBatchStmt<ParamValue> {

        private final String sql;

        private final List<List<ParamValue>> groupList;

        private ParamBatchStmtMin(String sql, List<List<ParamValue>> groupList) {
            this.sql = sql;
            this.groupList = JdbdCollections.unmodifiableList(groupList);
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<List<ParamValue>> getGroupList() {
            return this.groupList;
        }

        @Override
        public int getTimeout() {
            return 0;
        }


    }

    private static final class ParamStmtFull implements ParamStmt {

        private final String sql;

        private final List<? extends ParamValue> paramGroup;

        private final Consumer<ResultStates> statesConsumer;

        private final int timeout;

        private final int fetchSize;

        private final Function<Object, Publisher<byte[]>> importPublisher;

        private final Function<Object, Subscriber<byte[]>> exportSubscriber;

        private ParamStmtFull(String sql, List<? extends ParamValue> paramGroup
                , Consumer<ResultStates> statesConsumer, StatementOption option) {
            this.sql = sql;
            this.paramGroup = JdbdCollections.unmodifiableList(paramGroup);
            this.statesConsumer = statesConsumer;
            this.timeout = option.getTimeout();

            this.fetchSize = option.getFetchSize();
            this.importPublisher = option.getImportPublisher();
            this.exportSubscriber = option.getExportSubscriber();
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return this.statesConsumer;
        }

        @Override
        public List<? extends ParamValue> getBindGroup() {
            return this.paramGroup;
        }

        @Override
        public int getFetchSize() {
            return this.fetchSize;
        }

        @Override
        public int getTimeout() {
            return this.timeout;
        }

        @Nullable
        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return this.importPublisher;
        }

        @Nullable
        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return this.exportSubscriber;
        }

    }

    private static class ParamStmtMin extends AbstractMinStmt implements ParamStmt {

        private final String sql;

        private final List<? extends ParamValue> paramGroup;

        private ParamStmtMin(String sql, List<? extends ParamValue> paramGroup) {
            this.sql = sql;
            this.paramGroup = JdbdCollections.unmodifiableList(paramGroup);
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return JdbdFunctions.noActionConsumer();
        }

        @Override
        public List<? extends ParamValue> getBindGroup() {
            return this.paramGroup;
        }

        @Override
        public int getTimeout() {
            return 0;
        }


    }


    private static final class BatchStmtImpl extends AbstractMinStmt implements ParamBatchStmt<ParamValue> {

        private final String sql;

        private final List<List<ParamValue>> groupList;

        private final int timeOut;

        private BatchStmtImpl(String sql, List<List<ParamValue>> groupList, int timeOut) {
            this.sql = sql;
            this.groupList = JdbdCollections.unmodifiableList(groupList);
            this.timeOut = timeOut;
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<List<ParamValue>> getGroupList() {
            return this.groupList;
        }

        @Override
        public int getTimeout() {
            return this.timeOut;
        }

    }


    private static final class SimpleParamStmt extends AbstractMinStmt implements ParamStmt {

        private final String sql;

        private final List<? extends ParamValue> paramGroup;


        private SimpleParamStmt(String sql, List<? extends ParamValue> paramGroup) {
            this.sql = sql;
            this.paramGroup = Collections.unmodifiableList(paramGroup);
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<? extends ParamValue> getBindGroup() {
            return this.paramGroup;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return MultiResult.EMPTY_CONSUMER;
        }


        @Override
        public int getTimeout() {
            return 0;
        }

    }


    private static class StmtImpl2 extends AbstractMinStmt implements StaticStmt {

        private final String sql;

        private final int timeout;

        private StmtImpl2(String sql, int timeout) {
            this.sql = sql;
            this.timeout = timeout;
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public int getTimeout() {
            return this.timeout;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return MultiResult.EMPTY_CONSUMER;
        }


    }

    private static class StmtImpl2C extends AbstractMinStmt implements StaticStmt {

        private final String sql;

        private final Consumer<ResultStates> consumer;

        private StmtImpl2C(String sql, Consumer<ResultStates> consumer) {
            this.sql = sql;
            this.consumer = consumer;
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public int getTimeout() {
            return 0;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return this.consumer;
        }


    }


    private static class StmtImpl1 extends AbstractMinStmt implements StaticStmt {

        private final String sql;

        private StmtImpl1(String sql) {
            this.sql = sql;
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public int getTimeout() {
            return 0;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return MultiResult.EMPTY_CONSUMER;
        }

    }

    private static final class StaticStmtFull implements StaticStmt {

        private final String sql;

        private final Consumer<ResultStates> statesConsumer;

        private final int timeout;

        private final int fetchSize;

        private final Function<Object, Publisher<byte[]>> importPublisher;

        private final Function<Object, Subscriber<byte[]>> exportSubscriber;

        private StaticStmtFull(String sql, Consumer<ResultStates> statesConsumer, StatementOption option) {
            if (!JdbdStrings.hasText(sql)) {
                throw new IllegalArgumentException("sql must have text");
            }
            this.sql = sql;
            this.statesConsumer = statesConsumer;
            this.timeout = option.getTimeout();
            this.fetchSize = option.getFetchSize();

            this.importPublisher = option.getImportPublisher();
            this.exportSubscriber = option.getExportSubscriber();
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return this.statesConsumer;
        }

        @Override
        public int getTimeout() {
            return this.timeout;
        }

        @Override
        public int getFetchSize() {
            return this.fetchSize;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return this.importPublisher;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return this.exportSubscriber;
        }


    }

    private static final class BatchStmtFull implements StaticBatchStmt {

        private final List<String> sqlGroup;

        private final int timeout;

        private final int fetchSize;

        private final Function<Object, Publisher<byte[]>> importPublisher;

        private final Function<Object, Subscriber<byte[]>> exportSubscriber;

        private BatchStmtFull(List<String> sqlGroup, StatementOption option) {
            this.sqlGroup = Collections.unmodifiableList(sqlGroup);
            this.timeout = option.getTimeout();
            this.fetchSize = option.getFetchSize();

            this.importPublisher = option.getImportPublisher();
            this.exportSubscriber = option.getExportSubscriber();
        }

        @Override
        public List<String> getSqlGroup() {
            return this.sqlGroup;
        }

        @Override
        public int getTimeout() {
            return timeout;
        }

        @Override
        public int getFetchSize() {
            return this.sqlGroup.size() == 1 ? this.fetchSize : 0;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return this.sqlGroup.size() == 1 ? this.importPublisher : null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return this.sqlGroup.size() == 1 ? this.exportSubscriber : null;
        }

    }

    private static class StaticStmtWithExport implements StaticStmt {

        private final String sql;

        private final int timeout;

        private final Function<Object, Subscriber<byte[]>> exportFunction;

        private StaticStmtWithExport(String sql, int timeout, Function<Object, Subscriber<byte[]>> exportFunction) {
            this.sql = sql;
            this.timeout = timeout;
            this.exportFunction = exportFunction;
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return JdbdFunctions.noActionConsumer();
        }

        @Override
        public int getTimeout() {
            return this.timeout;
        }

        @Override
        public int getFetchSize() {
            return 0;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return this.exportFunction;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

    }


    private static class StmtImpl3 extends AbstractMinStmt implements StaticStmt {

        private final String sql;

        private final int timeout;

        private final Consumer<ResultStates> consumer;

        private StmtImpl3(String sql, int timeout, Consumer<ResultStates> consumer) {
            this.sql = sql;
            this.timeout = timeout;
            this.consumer = consumer;
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public int getTimeout() {
            return this.timeout;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return this.consumer;
        }


    }

    private static final class GroupStmtZeroTimeout extends AbstractMinStmt implements StaticBatchStmt {

        private final List<String> sqlGroup;

        private GroupStmtZeroTimeout(List<String> sqlGroup) {
            this.sqlGroup = Collections.unmodifiableList(sqlGroup);
        }

        @Override
        public List<String> getSqlGroup() {
            return this.sqlGroup;
        }

        @Override
        public int getTimeout() {
            return 0;
        }


    }

    private static final class GroupStmtImpl extends AbstractMinStmt implements StaticBatchStmt {

        private final List<String> sqlGroup;

        private final int timeout;

        private GroupStmtImpl(List<String> sqlGroup, int timeout) {
            this.sqlGroup = Collections.unmodifiableList(sqlGroup);
            this.timeout = timeout;
        }

        @Override
        public List<String> getSqlGroup() {
            return this.sqlGroup;
        }

        @Override
        public int getTimeout() {
            return this.timeout;
        }

    }


}
