package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.PrepareStmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.util.List;

/**
 * @see ExtendedStmtTask
 */
interface ExtendedCommandWriter {


    boolean isNeedPrepare();

    boolean supportFetch();

    boolean isNeedClose();

    @Nullable
    PostgreStmt getCache();

    int getFetchSize();

    void handlePrepareResponse(List<DataType> paramTypeList, @Nullable PgRowMeta rowMeta);

    /**
     * @throws IllegalStateException throw(not emit) when {@link #isOneRoundTrip()} return false.
     */
    Publisher<ByteBuf> prepare();

    /**
     * @throws IllegalStateException throw(not emit) when {@link #isOneRoundTrip()} return true.
     */
    Publisher<ByteBuf> executeOneRoundTrip();

    /**
     * @throws IllegalArgumentException throw(not emit) when stmt sql and {@link PrepareStmt#getSql()} not match.
     * @throws IllegalStateException    throw(not emit) when {@link #isOneRoundTrip()} return false.
     */
    Publisher<ByteBuf> bindAndExecute();

    /**
     * @throws IllegalStateException throw(not emit) when {@link #supportFetch()} return false.
     */
    Publisher<ByteBuf> fetch();

    /**
     * @throws IllegalStateException throw(not emit) when {@link #isNeedClose()} return false.
     */
    Publisher<ByteBuf> closeStatement();


}
