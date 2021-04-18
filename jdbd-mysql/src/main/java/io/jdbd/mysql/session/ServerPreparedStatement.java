package io.jdbd.mysql.session;

import io.jdbd.mysql.stmt.PrepareStmtTask;
import io.jdbd.result.MultiResults;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.PreparedStatement;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;

public final class ServerPreparedStatement implements PreparedStatement {

    public static PreparedStatement create(PrepareStmtTask task) {
        throw new UnsupportedOperationException();
    }


    @Override
    public void bind(int indexBasedZero, Object nullable) {

    }

    @Override
    public void addBatch() {

    }

    @Override
    public Publisher<ResultStates> executeBatch() {
        return null;
    }

    @Override
    public Publisher<ResultStates> executeUpdate() {
        return null;
    }

    @Override
    public Publisher<ResultRow> executeQuery() {
        return null;
    }

    @Override
    public Publisher<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer) {
        return null;
    }

    @Override
    public MultiResults executeMulti() {
        return null;
    }

    @Override
    public void setFetchSize(int fetchSize) {

    }

    @Override
    public boolean supportLongData() {
        return true;
    }


}
