package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import io.jdbd.mysql.JdbdMySQLException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface PreparedTask extends MySQLTask {

    /**
     * @throws JdbdMySQLException when task(prepared statement) not prepared yet.
     */
    MySQLColumnMeta[] obtainParameterMeta() throws JdbdMySQLException;

    int obtainPreparedWarningCount() throws IllegalStateException;

    MySQLColumnMeta[] obtainColumnMeta();

    <T> Flux<T> executeQuery(List<BindValue> parameterGroup, BiFunction<ResultRow, ResultRowMeta, T> decoder
            , Consumer<ResultStates> statesConsumer);

    Mono<ResultStates> executeUpdate(List<BindValue> parameterGroup);

    Flux<ResultStates> executeBatchUpdate(List<List<BindValue>> parameterGroupList);

    Mono<Void> close();

}
