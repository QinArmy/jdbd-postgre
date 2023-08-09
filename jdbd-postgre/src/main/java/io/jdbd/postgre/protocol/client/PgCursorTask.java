package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.result.ResultItem;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;

final class PgCursorTask extends PgCommandTask {


    /**
     * <p>
     * Just create task,don't submit task.
     * </p>
     */
    static PgCursorTask create(String cursorName, TaskAdjutant adjutant) {
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
