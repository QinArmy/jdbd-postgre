package io.jdbd.postgre.protocol.client;

import io.jdbd.vendor.stmt.ParamSingleStmt;
import io.jdbd.vendor.stmt.PrepareStmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;

/**
 * @see ExtendedStmtTask
 */
interface ExtendedCommandWriter {

    boolean isOneShot();

    boolean hasCache();

    boolean supportFetch();

    boolean needClose();

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
    Publisher<Iterable<ByteBuf>> bindExecute(ParamSingleStmt stmt);

    /**
     * @throws IllegalStateException throw(not emit) when {@link #supportFetch()} return false.
     */
    Publisher<ByteBuf> fetch();

    /**
     * @throws IllegalStateException throw(not emit) when {@link #needClose()} return false.
     */
    Publisher<ByteBuf> closeStatement();


}
