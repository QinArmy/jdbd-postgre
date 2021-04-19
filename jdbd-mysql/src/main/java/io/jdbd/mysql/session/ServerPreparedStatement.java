package io.jdbd.mysql.session;

import io.jdbd.mysql.stmt.PrepareStmtTask;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.ReactorPreparedStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

public final class ServerPreparedStatement implements ReactorPreparedStatement {

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
    public Flux<ResultStates> executeBatch() {
        return null;
    }

    @Override
    public Mono<ResultStates> executeUpdate() {
        return null;
    }

    @Override
    public Flux<ResultRow> executeQuery() {
        return null;
    }

    @Override
    public Flux<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer) {
        return null;
    }

    @Override
    public ReactorMultiResult executeMulti() {
        return null;
    }

    @Override
    public ReactorMultiResult executeBatchMulti() {
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
