package io.jdbd.postgre.stmt;

import io.jdbd.postgre.util.PgFunctions;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.stmt.JdbdStmts;
import io.jdbd.vendor.stmt.StatementOption;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class PgStmts extends JdbdStmts {

    public static BindStmt bindable(String sql, List<BindValue> paramGroup) {
        return new BindStmtTimeout(sql, paramGroup, 0);
    }

    public static BindStmt bindable(String sql, List<BindValue> paramGroup, Consumer<ResultStates> statesConsumer
            , StatementOption option) {
        return new BindStmtFull(sql, paramGroup, statesConsumer, option);
    }

    public static BindStmt bindable(String sql, List<BindValue> paramGroup
            , StatementOption option) {
        return new BindStmtFull(sql, paramGroup, PgFunctions.noActionConsumer(), option);
    }

    public static BindStmt bindable(String sql, BindValue param) {
        return new BindStmtMin(sql, param);
    }

    public static BatchBindStmt bindableBatch(String sql, List<List<BindValue>> groupList) {
        return new BatchBindStmtImpl(sql, groupList, 0);
    }

    public static BatchBindStmt bindableBatch(String sql, List<List<BindValue>> groupList, StatementOption option) {
        return new BatchBindStmtImpl(sql, groupList, 0);
    }

    public static BatchBindStmt bindableBatch(String sql, List<List<BindValue>> groupList, int timeout) {
        return new BatchBindStmtImpl(sql, groupList, timeout);
    }

    public static MultiBindStmt multi(List<BindStmt> stmtGroup) {
        return new MultiBindStmtImpl(stmtGroup, 0);
    }


    private static class BindStmtMin implements BindStmt {

        private final String sql;

        private final List<BindValue> paramGroup;

        private BindStmtMin(String sql, BindValue bindValue) {
            this.sql = sql;
            this.paramGroup = Collections.singletonList(bindValue);
        }

        private BindStmtMin(String sql, List<BindValue> paramGroup) {
            this.sql = sql;
            if (paramGroup.size() == 1) {
                this.paramGroup = Collections.singletonList(paramGroup.get(0));
            } else {
                this.paramGroup = Collections.unmodifiableList(paramGroup);
            }
        }

        @Override
        public final List<BindValue> getParamGroup() {
            return this.paramGroup;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return PgFunctions.noActionConsumer();
        }

        @Override
        public int getFetchSize() {
            return 0;
        }

        @Override
        public int getTimeout() {
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

    private static class BindStmtTimeout extends BindStmtMin {

        private final int timeout;

        private BindStmtTimeout(String sql, BindValue param, final int timeout) {
            super(sql, param);
            this.timeout = timeout;
        }

        private BindStmtTimeout(String sql, List<BindValue> paramGroup, final int timeout) {
            super(sql, paramGroup);
            this.timeout = timeout;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

    }

    private static class BindStmtQuery extends BindStmtTimeout {

        private final Consumer<ResultStates> stateConsumer;

        private final int fetchSize;

        private BindStmtQuery(String sql, BindValue param, Consumer<ResultStates> stateConsumer) {
            super(sql, param, 0);
            this.stateConsumer = stateConsumer;
            this.fetchSize = 0;
        }

        private BindStmtQuery(String sql, List<BindValue> paramGroup, Consumer<ResultStates> stateConsumer) {
            super(sql, paramGroup, 0);
            this.stateConsumer = stateConsumer;
            this.fetchSize = 0;
        }

        private BindStmtQuery(String sql, List<BindValue> paramGroup
                , Consumer<ResultStates> stateConsumer, StatementOption option) {
            super(sql, paramGroup, option.getTimeout());
            this.stateConsumer = stateConsumer;
            this.fetchSize = option.getFetchSize();
        }

        @Override
        public final int getFetchSize() {
            return this.fetchSize;
        }

        @Override
        public final Consumer<ResultStates> getStatusConsumer() {
            return this.stateConsumer;
        }
    }

    private static class BindStmtFull extends BindStmtQuery {

        private final Function<Object, Publisher<byte[]>> importPublisher;

        private final Function<Object, Subscriber<byte[]>> exportSubscriber;


        private BindStmtFull(String sql, List<BindValue> paramGroup, Consumer<ResultStates> stateConsumer
                , StatementOption option) {
            super(sql, paramGroup, stateConsumer, option);
            this.importPublisher = option.getImportFunction();
            this.exportSubscriber = option.getExportSubscriber();
        }


        @Override
        public final Function<Object, Publisher<byte[]>> getImportPublisher() {
            return this.importPublisher;
        }

        @Override
        public final Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return this.exportSubscriber;
        }

    }


    private static final class BatchBindStmtImpl implements BatchBindStmt {

        private final String sql;

        private final List<List<BindValue>> groupList;

        private final int timeout;


        private BatchBindStmtImpl(String sql, List<List<BindValue>> groupList, int timeout) {
            this.sql = sql;
            this.groupList = Collections.unmodifiableList(groupList);
            this.timeout = timeout;
        }

        @Override
        public final List<List<BindValue>> getGroupList() {
            return this.groupList;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }


    }

    private static final class MultiBindStmtImpl implements MultiBindStmt {

        private final List<BindStmt> stmtGroup;

        private final int timeout;

        private MultiBindStmtImpl(List<BindStmt> stmtGroup, int timeout) {
            this.stmtGroup = Collections.unmodifiableList(stmtGroup);
            this.timeout = timeout;
        }

        @Override
        public final List<BindStmt> getStmtGroup() {
            return this.stmtGroup;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

    }


}
