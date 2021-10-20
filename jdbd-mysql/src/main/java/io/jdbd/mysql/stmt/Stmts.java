package io.jdbd.mysql.stmt;

import io.jdbd.mysql.MySQLType;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.util.JdbdFunctions;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class Stmts extends JdbdStmts {


    public static StaticStmt stmt(String sql, StatementOption option) {
        return new MySQLOptionStaticStmt(sql, option);
    }

    public static StaticStmt stmt(String sql, Consumer<ResultStates> statesConsumer, StatementOption option) {
        return new MySQLQueryOptionStaticStmt(sql, statesConsumer, option);
    }

    public static StaticBatchStmt batch(List<String> sqlGroup, StatementOption option) {
        return new MySQLOptionStaticBatchStmt(sqlGroup, option);
    }

    public static StaticMultiStmt multiStmt(String multiSql, StatementOption option) {
        return new MySQLOptionStaticMultiStmt(multiSql, option);
    }

    public static ParamStmt paramStmt(String sql, List<ParamValue> bindGroup, StatementOption option) {
        return new MySQLOptionParamStmt<>(sql, bindGroup, option);
    }

    public static ParamStmt paramStmt(String sql, List<ParamValue> paramGroup
            , Consumer<ResultStates> statesConsumer, StatementOption option) {
        return new MySQLOptionQueryParamStmt<>(sql, paramGroup, statesConsumer, option);
    }

    public static ParamBatchStmt<ParamValue> paramBatch(String sql, List<List<ParamValue>> groupList
            , StatementOption option) {
        return new MySQLOptionParamBatchStmt<>(sql, groupList, option);
    }

    public static BindStmt single(String sql, BindValue bindValue) {
        return new MySQLSimpleBindStmt(sql, Collections.singletonList(bindValue));
    }

    public static BindStmt single(String sql, MySQLType type, @Nullable Object value) {
        return new MySQLSimpleBindStmt(sql, Collections.singletonList(BindValue.wrap(0, type, value)));
    }

    public static BindStmt bind(String sql, List<BindValue> bindGroup) {
        return new MySQLSimpleBindStmt(sql, bindGroup);
    }

    public static BindStmt bind(String sql, List<BindValue> bindGroup, Consumer<ResultStates> statesConsumer) {
        return new MySQLSimpleQueryBindStmt(sql, bindGroup, statesConsumer);
    }

    public static BindBatchStmt bindBatch(String sql, List<List<BindValue>> groupList) {
        return new MySQLSimpleBindBatchStmt(sql, groupList);
    }

    public static BindStmt bind(String sql, List<BindValue> bindGroup, StatementOption option) {
        return new MySQLOptionBindStmt(sql, bindGroup, option);
    }

    public static BindStmt bind(String sql, List<BindValue> bindGroup, Consumer<ResultStates> statesConsumer
            , StatementOption option) {
        return new MySQLOptionQueryBindStmt(sql, bindGroup, statesConsumer, option);
    }

    public static BindBatchStmt bindBatch(String sql, List<List<BindValue>> groupList, StatementOption option) {
        return new MySQLOptionBindBatchStmt(sql, groupList, option);
    }

    public static BindStmt elementOfMulti(String sql, List<BindValue> paramGroup) {
        return new MySQLElementBindStmt(sql, paramGroup);
    }


    public static BindMultiStmt multi(List<BindStmt> stmtList) {
        return new MySQLSimpleBindMultiStmt(stmtList);
    }

    public static BindMultiStmt multi(List<BindStmt> stmtList, StatementOption option) {
        return new MySQLOptionBindMultiStmt(stmtList, option);
    }

    @Deprecated
    public static BindBatchStmt batchBind(String sql, List<List<BindValue>> paramGroupList) {
        throw new UnsupportedOperationException();
    }



    private static Map<String, QueryAttr> wrapQueryAttrs(final Map<String, QueryAttr> attrMap) {
        final Map<String, QueryAttr> queryAttrMap;
        if (attrMap.size() == 0) {
            queryAttrMap = Collections.emptyMap();
        } else {
            queryAttrMap = Collections.unmodifiableMap(attrMap);
        }
        return queryAttrMap;
    }


    private static class MySQLOptionStaticStmt extends OptionStaticStmt implements MySQLStmt {

        private final Map<String, QueryAttr> queryAttrs;

        private MySQLOptionStaticStmt(final String sql, final StatementOption option) {
            super(sql, option);
            this.queryAttrs = wrapQueryAttrs(option.getAttrGroup());
        }

        @Override
        public final Map<String, QueryAttr> getQueryAttrs() {
            return this.queryAttrs;
        }

    }

    private static final class MySQLQueryOptionStaticStmt extends OptionQueryStaticStmt
            implements MySQLStmt {


        private final Map<String, QueryAttr> queryAttrs;


        private MySQLQueryOptionStaticStmt(String sql, Consumer<ResultStates> statesConsumer, StatementOption option) {
            super(sql, statesConsumer, option);
            this.queryAttrs = wrapQueryAttrs(option.getAttrGroup());
        }

        @Override
        public Map<String, QueryAttr> getQueryAttrs() {
            return this.queryAttrs;
        }


    }

    private static final class MySQLOptionStaticBatchStmt extends OptionStaticBatchStmt
            implements MySQLStmt {

        private final Map<String, QueryAttr> queryAttrs;

        private MySQLOptionStaticBatchStmt(List<String> sqlGroup, StatementOption option) {
            super(sqlGroup, option);
            this.queryAttrs = wrapQueryAttrs(option.getAttrGroup());
        }

        @Override
        public Map<String, QueryAttr> getQueryAttrs() {
            return this.queryAttrs;
        }

    }

    private static final class MySQLOptionStaticMultiStmt extends OptionStaticMultiStmt implements StaticMultiStmt
            , MySQLStmt {

        private final Map<String, QueryAttr> queryAttrs;

        private MySQLOptionStaticMultiStmt(final String multiStmt, final StatementOption option) {
            super(multiStmt, option);
            this.queryAttrs = wrapQueryAttrs(option.getAttrGroup());
        }


        @Override
        public Map<String, QueryAttr> getQueryAttrs() {
            return this.queryAttrs;
        }


    }


    private static class MySQLOptionParamStmt<T extends ParamValue> extends OptionParamStmt<T>
            implements MySQLStmt {

        private final Map<String, QueryAttr> queryAttrs;

        private MySQLOptionParamStmt(final String sql, final List<T> bindGroup, final StatementOption option) {
            super(sql, bindGroup, option);
            this.queryAttrs = wrapQueryAttrs(option.getAttrGroup());
        }

        @Override
        public final Map<String, QueryAttr> getQueryAttrs() {
            return this.queryAttrs;
        }

        @Override
        public final Consumer<ResultStates> getStatusConsumer() {
            return JdbdFunctions.noActionConsumer();
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

    private static class MySQLOptionQueryParamStmt<T extends ParamValue> extends OptionQueryParamStmt<T>
            implements MySQLStmt {

        private final Map<String, QueryAttr> queryAttrs;

        private MySQLOptionQueryParamStmt(String sql, List<T> bindGroup
                , Consumer<ResultStates> statesConsumer, StatementOption option) {
            super(sql, bindGroup, statesConsumer, option);
            this.queryAttrs = wrapQueryAttrs(option.getAttrGroup());

        }

        @Override
        public final Map<String, QueryAttr> getQueryAttrs() {
            return this.queryAttrs;
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


    private static class MySQLOptionParamBatchStmt<T extends ParamValue> extends OptionParamBatchStmt<T>
            implements MySQLStmt {

        private final Map<String, QueryAttr> queryAttrs;

        private MySQLOptionParamBatchStmt(final String sql, final List<List<T>> groupList
                , final StatementOption option) {
            super(sql, groupList, option);
            this.queryAttrs = wrapQueryAttrs(option.getAttrGroup());

        }

        @Override
        public final Map<String, QueryAttr> getQueryAttrs() {
            return this.queryAttrs;
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

    private static final class MySQLSimpleBindStmt extends SimpleParamStmt<BindValue> implements BindStmt {

        private MySQLSimpleBindStmt(String sql, List<BindValue> bindGroup) {
            super(sql, bindGroup);
        }

    }

    private static final class MySQLSimpleQueryBindStmt extends SimpleQueryParamStmt<BindValue> implements BindStmt {

        private MySQLSimpleQueryBindStmt(String sql, List<BindValue> bindGroup
                , Consumer<ResultStates> statesConsumer) {
            super(sql, bindGroup, statesConsumer);
        }

    }

    private static final class MySQLSimpleBindBatchStmt extends SimpleParamBatchStmt<BindValue>
            implements BindBatchStmt {

        private MySQLSimpleBindBatchStmt(String sql, List<List<BindValue>> groupList) {
            super(sql, groupList);
        }

    }


    private static class MySQLOptionBindStmt extends MySQLOptionParamStmt<BindValue> implements BindStmt {

        private MySQLOptionBindStmt(String sql, List<BindValue> bindGroup, StatementOption option) {
            super(sql, bindGroup, option);
        }

    }

    private static final class MySQLOptionQueryBindStmt extends MySQLOptionQueryParamStmt<BindValue>
            implements BindStmt {

        private MySQLOptionQueryBindStmt(String sql, List<BindValue> bindGroup
                , Consumer<ResultStates> statesConsumer, StatementOption option) {
            super(sql, bindGroup, statesConsumer, option);
        }

    }

    private static final class MySQLOptionBindBatchStmt extends MySQLOptionParamBatchStmt<BindValue>
            implements BindBatchStmt {

        private MySQLOptionBindBatchStmt(String sql, List<List<BindValue>> groupList, StatementOption option) {
            super(sql, groupList, option);
        }

    }

    private static final class MySQLElementBindStmt extends ElementBindStmt<BindValue> implements BindStmt {

        private MySQLElementBindStmt(String sql, List<BindValue> bindGroup) {
            super(sql, bindGroup);
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


    private static final class MySQLSimpleBindMultiStmt implements BindMultiStmt {

        private final List<BindStmt> stmtGroup;

        private MySQLSimpleBindMultiStmt(List<BindStmt> stmtGroup) {
            this.stmtGroup = wrapGroup(stmtGroup);
        }

        @Override
        public List<BindStmt> getStmtGroup() {
            return this.stmtGroup;
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

    private static final class MySQLOptionBindMultiStmt implements BindMultiStmt {

        private final List<BindStmt> stmtList;

        private final int timeout;

        public MySQLOptionBindMultiStmt(List<BindStmt> stmtList, StatementOption option) {
            this.stmtList = wrapGroup(stmtList);
            this.timeout = option.getTimeout();
        }

        @Override
        public List<BindStmt> getStmtGroup() {
            return this.stmtList;
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
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }

    }


}
