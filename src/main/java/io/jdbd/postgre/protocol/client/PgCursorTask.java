package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.result.*;
import io.jdbd.session.Option;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

final class PgCursorTask extends PgCommandTask implements CursorTask {


    /**
     * <p>
     * Just create task,don't submit task.
     * </p>
     */
    static PgCursorTask create(String cursorName, Function<Option<?>, ?> optionFunc, TaskAdjutant adjutant) {
        return new PgCursorTask(cursorName, adjutant);
    }

    private static final Logger LOG = LoggerFactory.getLogger(PgCursorTask.class);

    private final String cursorName;

    private final String safeCursorName;

    private PgCursorTask(String cursorName, TaskAdjutant adjutant) {
        super(adjutant);
        this.cursorName = cursorName;
        this.safeCursorName = cursorName;
    }


    @Override
    public <T> Flux<T> fetch(CursorDirection direction, long count, Function<CurrentRow, T> function, Consumer<ResultStates> consumer) {
        return null;
    }

    @Override
    public OrderedFlux fetch(CursorDirection direction, long count, boolean close) {
        return null;
    }

    @Override
    public Mono<ResultStates> move(CursorDirection direction, long count) {
        return null;
    }

    @Override
    public boolean isAutoCloseCursorOnError() {
        return false;
    }

    @Override
    public <T> Mono<T> close() {
        return null;
    }

    @Override
    public void next(ResultItem result) {

    }

    @Override
    Stmt getStmt() {
        return null;
    }


    @Override
    Logger getLog() {
        return LOG;
    }

    @Override
    void internalToString(StringBuilder builder) {

    }

    @Override
    boolean handlePrepareResponse(List<PgType> paramTypeList, ResultRowMeta rowMeta) {
        return false;
    }

    @Override
    boolean handleClientTimeout() {
        return false;
    }

    @Override
    protected Action onError(Throwable e) {
        return null;
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
    protected void emitError(final Throwable e) {
        super.emitError(e);
    }


}
