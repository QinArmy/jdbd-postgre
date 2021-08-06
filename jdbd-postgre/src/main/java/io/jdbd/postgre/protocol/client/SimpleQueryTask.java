package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.result.SingleResult;
import io.jdbd.vendor.result.JdbdMultiResults;
import io.jdbd.vendor.result.MultiResultSink;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;


final class SimpleQueryTask extends PgTask {


    static Mono<ResultState> update(Stmt stmt, TaskAdjutant adjutant) {
        return JdbdMultiResults.update(adjutant, asFlux(Collections.singletonList(stmt), adjutant));
    }

    static Flux<ResultRow> query(Stmt stmt, TaskAdjutant adjutant) {
        return JdbdMultiResults.query(adjutant, asFlux(Collections.singletonList(stmt), adjutant));
    }

    static Flux<SingleResult> asFlux(List<Stmt> stmtList, TaskAdjutant adjutant) {
        return JdbdMultiResults.createAsFlux(adjutant, sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmtList, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }


    private final DownstreamSink downstreamSink;

    private SimpleQueryTask(List<Stmt> stmtList, MultiResultSink sink, TaskAdjutant adjutant) {
        super(adjutant);
        this.downstreamSink = new BatchMultiResultSink(sink);
    }


    @Override
    protected Publisher<ByteBuf> start() {
        return null;
    }

    @Override
    protected boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        return false;
    }

    @Override
    protected Action onError(Throwable e) {
        return null;
    }


    /*################################## blow private instance class ##################################*/

    private interface DownstreamSink {


    }

    private abstract class AbstractDownstreamSink implements DownstreamSink {


    }

    private final class BatchMultiResultSink extends AbstractDownstreamSink {

        private final MultiResultSink sink;

        private BatchMultiResultSink(MultiResultSink sink) {
            this.sink = sink;
        }

    }


}
