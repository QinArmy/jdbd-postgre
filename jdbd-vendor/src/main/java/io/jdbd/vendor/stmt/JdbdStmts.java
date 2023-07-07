package io.jdbd.vendor.stmt;


import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.util.JdbdCollections;
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

    public static final Consumer<ResultStates> IGNORE_RESULT_STATES = JdbdStmts::ignoreResultStates;

    protected static final List<ParamValue> EMPTY_PARAM_GROUP = Collections.emptyList();


    public static StaticStmt stmt(final String sql) {
        return new SimpleStaticStmt(sql);
    }

    public static StaticStmt stmt(final String sql, Consumer<ResultStates> statusConsumer) {
        return new SimpleQueryStaticStmt(sql, statusConsumer);
    }

    public static StaticBatchStmt batch(List<String> sqlGroup) {
        return new SimpleStaticBatchStmt(sqlGroup);
    }

    public static StaticMultiStmt multiStmt(String multiStmt) {
        return new SimpleStaticMultiStmt(multiStmt);
    }

    public static StaticStmt stmt(String sql, StmtOption option) {
        Objects.requireNonNull(sql, "sql");
        return new OptionStaticStmt(sql, option);
    }

    public static StaticStmt stmt(String sql, Consumer<ResultStates> statesConsumer, StmtOption option) {
        Objects.requireNonNull(sql, "sql");
        return new OptionQueryStaticStmt(sql, statesConsumer, option);
    }

    public static StaticBatchStmt batch(final List<String> sqlGroup, final StmtOption option) {
        return new OptionStaticBatchStmt(sqlGroup, option);
    }


    public static ParamStmt single(String sql, ParamStmt bindValue) {
        throw new UnsupportedOperationException();
    }

    public static ParamStmt single(String sql, DataType type, @Nullable Object value) {
        throw new UnsupportedOperationException();
    }

    public static StaticMultiStmt multiStmt(String multiStmt, StmtOption option) {
        return new OptionStaticMultiStmt(multiStmt, option);
    }

    public static ParamStmt paramStmt(final String sql, @Nullable List<ParamValue> paramGroup) {
        if (paramGroup == null) {
            paramGroup = EMPTY_PARAM_GROUP;
        } else {
            paramGroup = JdbdCollections.unmodifiableList(paramGroup);
        }
        return new SimpleParamStmt<>(sql, paramGroup);
    }

    public static ParamStmt paramStmt(String sql, List<ParamValue> paramGroup, Consumer<ResultStates> statesConsumer) {
        return new SimpleQueryParamStmt<>(sql, paramGroup, statesConsumer);
    }

    public static ParamBatchStmt paramBatch(String sql, List<List<ParamValue>> groupList) {
        return new SimpleParamBatchStmt<>(sql, groupList);
    }

    public static ParamStmt paramStmt(final String sql, @Nullable List<ParamValue> paramGroup, StmtOption option) {
        if (paramGroup == null) {
            paramGroup = EMPTY_PARAM_GROUP;
        } else {
            paramGroup = JdbdCollections.unmodifiableList(paramGroup);
        }
        return new OptionParamStmt<>(sql, paramGroup, option);
    }

    public static ParamStmt paramStmt(String sql, List<ParamValue> paramGroup
            , Consumer<ResultStates> statesConsumer, StmtOption option) {
        return new OptionQueryParamStmt<>(sql, paramGroup, statesConsumer, option);
    }

    public static ParamBatchStmt<ParamValue> paramBatch(String sql, List<List<ParamValue>> groupList
            , StmtOption option) {
        return new OptionParamBatchStmt<>(sql, groupList, option);
    }

    public static ParamMultiStmt multiStmt(List<ParamStmt> stmtList, StmtOption option) {
        throw new UnsupportedOperationException();
    }


    @Deprecated
    public static ParamBatchStmt<ParamValue> batch(String sql, List<List<ParamValue>> groupList) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static ParamBatchStmt<ParamValue> batch(String sql, List<List<ParamValue>> groupList, int timeOut) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static StaticStmt stmtWithExport(String sql, Function<Object, Subscriber<byte[]>> function) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static StaticStmt stmt(String sql, int timeout) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static StaticStmt stmt(String sql, Consumer<ResultStates> statusConsumer, int timeout) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static List<StaticStmt> stmts(List<String> sqlList) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static List<StaticStmt> stmts(List<String> sqlList, int timeout) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static StaticBatchStmt group(List<String> sqlGroup, int timeout) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static ParamStmt multiPrepare(String sql, List<? extends ParamValue> paramGroup) {
        throw new UnsupportedOperationException();
    }


    public static <T extends ParamValue> List<T> wrapBindGroup(final List<T> bindGroup) {
        final List<T> newBindGroup;
        switch (bindGroup.size()) {
            case 0: {
                newBindGroup = Collections.emptyList();
            }
            break;
            case 1: {
                newBindGroup = Collections.singletonList(bindGroup.get(0));
            }
            break;
            default: {
                newBindGroup = Collections.unmodifiableList(bindGroup);
            }
        }
        return newBindGroup;
    }

    protected static <T> List<List<T>> wrapGroupList(final List<List<T>> groupList) {
        final List<List<T>> newGroupList;
        switch (groupList.size()) {
            case 0: {
                throw new IllegalArgumentException("groupList is empty.");
            }
            case 1: {
                newGroupList = Collections.singletonList(groupList.get(0));
            }
            break;
            default: {
                newGroupList = Collections.unmodifiableList(groupList);
            }
        }
        return newGroupList;
    }


    protected static <T> List<T> wrapGroup(final List<T> group) {
        Objects.requireNonNull(group, "group");
        final List<T> newGroup;
        switch (group.size()) {
            case 0:
                throw new IllegalArgumentException("group must not empty.");
            case 1: {
                final T e = group.get(0);
                if (e == null) {
                    throw new NullPointerException("group as index[0] is null");
                }
                newGroup = Collections.singletonList(e);
            }
            break;
            default: {
                final List<T> list = new ArrayList<>(group.size());
                final int size = group.size();
                T e;
                for (int i = 0; i < size; i++) {
                    e = group.get(i);
                    if (e == null) {
                        throw new NullPointerException(String.format("group at index[%s] is null.", i));
                    }
                    list.add(e);
                }
                newGroup = Collections.unmodifiableList(list);
            }
        }
        return newGroup;
    }


    private static class SimpleStaticStmt implements StaticStmt {

        private final String sql;

        SimpleStaticStmt(String sql) {
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
        public final int getFetchSize() {
            return 0;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return IGNORE_RESULT_STATES;
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

    private static final class SimpleQueryStaticStmt extends SimpleStaticStmt {

        private final Consumer<ResultStates> statesConsumer;

        private SimpleQueryStaticStmt(String sql, Consumer<ResultStates> statesConsumer) {
            super(sql);
            this.statesConsumer = statesConsumer;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return this.statesConsumer;
        }

    }

    private static final class SimpleStaticBatchStmt implements StaticBatchStmt {

        private final List<String> sqlGroup;

        private SimpleStaticBatchStmt(final List<String> sqlGroup) {
            this.sqlGroup = wrapGroup(sqlGroup);
        }

        @Override
        public List<String> getSqlGroup() {
            return this.sqlGroup;
        }

        @Override
        public int getTimeout() {
            return 0;
        }

        @Override
        public int getFetchSize() {
            return 0;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }

    }

    private static final class SimpleStaticMultiStmt implements StaticMultiStmt {

        private final String multiStmt;

        private SimpleStaticMultiStmt(String multiStmt) {
            this.multiStmt = multiStmt;
        }

        @Override
        public String getMultiStmt() {
            return this.multiStmt;
        }

        @Override
        public int getTimeout() {
            return 0;
        }

        @Override
        public int getFetchSize() {
            return 0;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }

    }


    protected static class OptionStaticStmt implements StaticStmt {

        private final String sql;

        private final int timeout;

        protected OptionStaticStmt(String sql, StmtOption option) {
            this.sql = sql;
            this.timeout = option.getTimeout();
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
        public int getFetchSize() {
            return 0;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return IGNORE_RESULT_STATES;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }
    }

    protected static class OptionQueryStaticStmt extends OptionStaticStmt {

        private final int fetchSize;

        private final Consumer<ResultStates> statesConsumer;

        protected OptionQueryStaticStmt(String sql, Consumer<ResultStates> statesConsumer
                , StmtOption option) {
            super(sql, option);
            this.fetchSize = option.getFetchSize();
            this.statesConsumer = statesConsumer;
        }

        @Override
        public final int getFetchSize() {
            return this.fetchSize;
        }

        @Override
        public final Consumer<ResultStates> getStatusConsumer() {
            return this.statesConsumer;
        }

    }


    protected static class OptionStaticBatchStmt implements StaticBatchStmt {

        protected final List<String> sqlGroup;

        private final int timeout;

        private final int fetchSize;

        protected OptionStaticBatchStmt(final List<String> sqlGroup, StmtOption option) {
            this.sqlGroup = wrapGroup(sqlGroup);
            this.timeout = option.getTimeout();
            this.fetchSize = this.sqlGroup.size() == 1 ? option.getFetchSize() : 0;
        }

        @Override
        public final List<String> getSqlGroup() {
            return this.sqlGroup;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

        @Override
        public final int getFetchSize() {
            return this.fetchSize;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }

    }

    protected static class OptionStaticMultiStmt implements StaticMultiStmt {

        private final String multiStmt;

        private final int timeout;

        protected OptionStaticMultiStmt(String multiStmt, StmtOption option) {
            this.multiStmt = multiStmt;
            this.timeout = option.getTimeout();

        }

        @Override
        public final String getMultiStmt() {
            return this.multiStmt;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

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

    protected static class SimpleParamStmt<T extends ParamValue> implements ParamStmt {

        private final String sql;

        protected final List<T> bindGroup;

        protected SimpleParamStmt(String sql, List<T> bindGroup) {
            this.sql = sql;
            this.bindGroup = bindGroup;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final List<T> getBindGroup() {
            return this.bindGroup;
        }

        @Override
        public final int getTimeout() {
            return 0;
        }

        @Override
        public final int getFetchSize() {
            return 0;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return IGNORE_RESULT_STATES;
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

    protected static class SimpleQueryParamStmt<T extends ParamValue> extends SimpleParamStmt<T> {

        private final Consumer<ResultStates> statesConsumer;

        protected SimpleQueryParamStmt(String sql, List<T> bindGroup
                , Consumer<ResultStates> statesConsumer) {
            super(sql, bindGroup);
            this.statesConsumer = statesConsumer;
        }

        @Override
        public final Consumer<ResultStates> getStatusConsumer() {
            return this.statesConsumer;
        }

    }

    protected static class SimpleParamBatchStmt<T extends ParamValue> implements ParamBatchStmt<T> {

        private final String sql;

        protected final List<List<T>> groupList;

        protected SimpleParamBatchStmt(String sql, List<List<T>> groupList) {
            this.sql = sql;
            this.groupList = wrapGroupList(groupList);
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final List<List<T>> getGroupList() {
            return this.groupList;
        }

        @Override
        public final int getTimeout() {
            return 0;
        }

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


    protected static class OptionParamStmt<T extends ParamValue> implements ParamStmt {

        private final String sql;

        protected final List<T> bindGroup;

        private final int timeout;

        protected OptionParamStmt(String sql, List<T> bindGroup, StmtOption option) {
            this.sql = sql;
            this.bindGroup = JdbdCollections.unmodifiableList(bindGroup);
            this.timeout = option.getTimeout();

        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return IGNORE_RESULT_STATES;
        }

        @Override
        public final List<T> getBindGroup() {
            return this.bindGroup;
        }


        @Override
        public final int getTimeout() {
            return this.timeout;
        }

        @Override
        public int getFetchSize() {
            return 0;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }


    }

    protected static class OptionQueryParamStmt<T extends ParamValue> extends OptionParamStmt<T> {

        private final int fetchSize;

        private final Consumer<ResultStates> statesConsumer;

        protected OptionQueryParamStmt(String sql, List<T> bindGroup
                , Consumer<ResultStates> statesConsumer, StmtOption option) {
            super(sql, bindGroup, option);
            this.fetchSize = option.getFetchSize();
            this.statesConsumer = statesConsumer;
        }

        @Override
        public final Consumer<ResultStates> getStatusConsumer() {
            return this.statesConsumer;
        }

        @Override
        public final int getFetchSize() {
            return this.fetchSize;
        }

    }

    protected static class OptionParamBatchStmt<T extends ParamValue> implements ParamBatchStmt<T> {

        private final String sql;

        protected final List<List<T>> groupList;

        private final int timeout;

        private final int fetchSize;

        protected OptionParamBatchStmt(final String sql, final List<List<T>> groupList
                , final StmtOption option) {
            this.sql = sql;
            this.timeout = option.getTimeout();
            this.groupList = wrapGroupList(groupList);
            this.fetchSize = this.groupList.size() == 1 ? option.getFetchSize() : 0;

        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final List<List<T>> getGroupList() {
            return this.groupList;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

        @Override
        public final int getFetchSize() {
            return this.fetchSize;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }


    }


    protected static class ElementBindStmt<T extends ParamValue> implements ParamStmt {

        private final String sql;

        protected final List<T> bindGroup;

        protected ElementBindStmt(String sql, List<T> bindGroup) {
            this.sql = sql;
            //only use Collections.unmodifiableList(List), can't use wrapGroup(List) .
            //because for io.jdbd.stmt.MultiStatement.addStatement() method.
            this.bindGroup = Collections.unmodifiableList(bindGroup);
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final List<T> getBindGroup() {
            return this.bindGroup;
        }

        @Override
        public final int getTimeout() {
            return 0;
        }

        @Override
        public final Consumer<ResultStates> getStatusConsumer() {
            return IGNORE_RESULT_STATES;
        }

        @Override
        public final int getFetchSize() {
            return 0;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }

    }


    private static void ignoreResultStates(ResultStates states) {
        //no-op
    }

}
