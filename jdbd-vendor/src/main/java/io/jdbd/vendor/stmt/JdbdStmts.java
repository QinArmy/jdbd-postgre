package io.jdbd.vendor.stmt;


import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultState;
import io.jdbd.vendor.util.JdbdCollections;
import io.jdbd.vendor.util.JdbdFunctions;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

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


    public static BatchParamStmt<ParamValue> batch(String sql, List<List<ParamValue>> groupList) {
        if (groupList.size() < 1) {
            throw new IllegalArgumentException("groupList is empty.");
        }
        return new BatchStmtImpl(sql, groupList, 0);
    }

    public static BatchParamStmt<ParamValue> batch(String sql, List<List<ParamValue>> groupList, int timeOut) {
        if (groupList.size() < 2) {
            throw new IllegalArgumentException("groupList size < 2");
        }
        return new BatchStmtImpl(sql, groupList, timeOut);
    }

    public static StaticStmt stmt(String sql) {
        Objects.requireNonNull(sql, "sql");
        return new StmtImpl1(sql);
    }

    public static StaticStmt stmtWithImport(String sql, Function<String, Publisher<byte[]>> function) {
        return new StmtImpImport2(sql, function);
    }

    public static StaticStmt stmtWithExport(String sql, Function<Object, Subscriber<byte[]>> function) {
        return new StaticStmtWithExport(sql, 0, function);
    }

    public static StaticStmt stmt(String sql, int timeout) {
        Objects.requireNonNull(sql, "sql");
        return timeout > 0 ? new StmtImpl2(sql, timeout) : new StmtImpl1(sql);
    }

    public static StaticStmt stmt(String sql, Consumer<ResultState> statusConsumer) {
        Objects.requireNonNull(sql, "sql");
        return new StmtImpl2C(sql, statusConsumer);
    }

    public static StaticStmt stmt(String sql, Consumer<ResultState> statusConsumer, int timeout) {
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

    public static MultiSqlStmt multiSqlStmt(String multiSql) {
        return new MultiSqlStmtImpl(multiSql, 0);
    }

    public static GroupStmt group(List<String> sqlGroup, int timeout) {
        return timeout == 0 ? new GroupStmtZeroTimeout(sqlGroup) : new GroupStmtImpl(sqlGroup, timeout);
    }

    public static GroupStmt group(List<String> sqlGroup) {
        return new GroupStmtZeroTimeout(sqlGroup);
    }

    public static ParamStmt singlePrepare(String sql, ParamValue paramValue) {
        return new SimpleParamStmt(sql, paramValue);
    }

    public static ParamStmt multiPrepare(String sql, List<? extends ParamValue> paramGroup) {
        return new SimpleParamStmt(sql, paramGroup);
    }


    private static final class BatchStmtImpl implements BatchParamStmt<ParamValue> {

        private final String sql;

        private final List<List<ParamValue>> groupList;

        private final int timeOut;

        private BatchStmtImpl(String sql, List<List<ParamValue>> groupList, int timeOut) {
            this.sql = sql;
            this.groupList = JdbdCollections.unmodifiableList(groupList);
            this.timeOut = timeOut;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final List<List<ParamValue>> getGroupList() {
            return this.groupList;
        }

        @Override
        public final int getTimeout() {
            return this.timeOut;
        }

    }


    private static final class SimpleParamStmt implements ParamStmt {

        private final String sql;

        private final List<? extends ParamValue> paramGroup;

        private SimpleParamStmt(String sql, ParamValue paramValue) {
            this.sql = sql;
            this.paramGroup = Collections.singletonList(paramValue);
        }

        private SimpleParamStmt(String sql, List<? extends ParamValue> paramGroup) {
            this.sql = sql;
            this.paramGroup = Collections.unmodifiableList(paramGroup);
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<? extends ParamValue> getParamGroup() {
            return this.paramGroup;
        }

        @Override
        public Consumer<ResultState> getStatusConsumer() {
            return MultiResult.EMPTY_CONSUMER;
        }

        @Override
        public int getFetchSize() {
            return 0;
        }

        @Override
        public int getTimeout() {
            return 0;
        }
    }


    private static class StmtImpl2 implements StaticStmt {

        private final String sql;

        private final int timeout;

        private StmtImpl2(String sql, int timeout) {
            this.sql = sql;
            this.timeout = timeout;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

        @Override
        public final Consumer<ResultState> getStatusConsumer() {
            return MultiResult.EMPTY_CONSUMER;
        }

    }

    private static class StmtImpl2C implements StaticStmt {

        private final String sql;

        private final Consumer<ResultState> consumer;

        private StmtImpl2C(String sql, Consumer<ResultState> consumer) {
            this.sql = sql;
            this.consumer = consumer;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final int getTimeout() {
            return 0;
        }

        @Override
        public final Consumer<ResultState> getStatusConsumer() {
            return this.consumer;
        }

    }


    private static class StmtImpl1 implements StaticStmt {

        private final String sql;

        private StmtImpl1(String sql) {
            this.sql = sql;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final int getTimeout() {
            return 0;
        }

        @Override
        public final Consumer<ResultState> getStatusConsumer() {
            return MultiResult.EMPTY_CONSUMER;
        }

        @Override
        public final Function<String, Publisher<byte[]>> getImportFunction() {
            return null;
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
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final Consumer<ResultState> getStatusConsumer() {
            return JdbdFunctions.noActionConsumer();
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

        @Override
        public final Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return this.exportFunction;
        }

    }

    private static class StmtImpImport2 implements StaticStmt {

        private final String sql;

        private final Function<String, Publisher<byte[]>> function;

        private StmtImpImport2(String sql, Function<String, Publisher<byte[]>> function) {
            this.sql = sql;
            this.function = Objects.requireNonNull(function, "function");
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final Consumer<ResultState> getStatusConsumer() {
            return JdbdFunctions.noActionConsumer();
        }

        @Override
        public final int getTimeout() {
            return 0;
        }

        @Override
        public final Function<String, Publisher<byte[]>> getImportFunction() {
            return this.function;
        }


    }


    private static class StmtImpl3 implements StaticStmt {

        private final String sql;

        private final int timeout;

        private final Consumer<ResultState> consumer;

        private StmtImpl3(String sql, int timeout, Consumer<ResultState> consumer) {
            this.sql = sql;
            this.timeout = timeout;
            this.consumer = consumer;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

        @Override
        public final Consumer<ResultState> getStatusConsumer() {
            return this.consumer;
        }


    }

    private static final class GroupStmtZeroTimeout implements GroupStmt {

        private final List<String> sqlGroup;

        private GroupStmtZeroTimeout(List<String> sqlGroup) {
            this.sqlGroup = Collections.unmodifiableList(sqlGroup);
        }

        @Override
        public final List<String> getSqlGroup() {
            return this.sqlGroup;
        }

        @Override
        public final int getTimeout() {
            return 0;
        }

    }

    private static final class GroupStmtImpl implements GroupStmt {

        private final List<String> sqlGroup;

        private final int timeout;

        private GroupStmtImpl(List<String> sqlGroup, int timeout) {
            this.sqlGroup = Collections.unmodifiableList(sqlGroup);
            this.timeout = timeout;
        }

        @Override
        public final List<String> getSqlGroup() {
            return this.sqlGroup;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

    }

    private static final class MultiSqlStmtImpl implements MultiSqlStmt {


        private final String multiSql;

        private final int timeout;

        private MultiSqlStmtImpl(String multiSql, int timeout) {
            this.multiSql = multiSql;
            this.timeout = timeout;
        }

        @Override
        public final String getMultiSql() {
            return this.multiSql;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

    }


}
