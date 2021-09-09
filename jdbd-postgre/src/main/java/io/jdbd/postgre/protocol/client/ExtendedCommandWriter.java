package io.jdbd.postgre.protocol.client;

import io.jdbd.vendor.stmt.PrepareStmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

/**
 * @see ExtendedStmtTask
 */
interface ExtendedCommandWriter {

    boolean isOneShot();

    boolean supportFetch();

    boolean needClose();

    @Nullable
    CachePrepare getCache();

    String getReplacedSql();

    String getStatementName();

    /**
     * @throws IllegalStateException throw(not emit) when {@link #isOneShot()} return false.
     */
    Publisher<ByteBuf> prepare();

    /**
     * @throws IllegalStateException throw(not emit) when {@link #isOneShot()} return true.
     */
    Publisher<ByteBuf> executeOneShot();

    /**
     * @throws IllegalArgumentException throw(not emit) when stmt sql and {@link PrepareStmt#getSql()} not match.
     * @throws IllegalStateException    throw(not emit) when {@link #isOneShot()} return false.
     */
    Publisher<ByteBuf> bindAndExecute();

    /**
     * @throws IllegalStateException throw(not emit) when {@link #supportFetch()} return false.
     */
    Publisher<ByteBuf> fetch();

    /**
     * @throws IllegalStateException throw(not emit) when {@link #needClose()} return false.
     */
    Publisher<ByteBuf> closeStatement();


}
