package io.jdbd.postgre.protocol.client;

import io.jdbd.vendor.stmt.ParamSingleStmt;
import io.jdbd.vendor.stmt.PrepareStmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

/**
 * @see ExtendedStmtTask
 */
interface ExtendedCommandWriter {

    /**
     * <p>
     * {@link ParamSingleStmt} is one shot ,If {@link ParamSingleStmt#getSql()} method of
     * {@link ExtendedStmtTask#getStmt()} no parameter placeholder,
     * and satisfy one of below conditions:
     * <ul>
     *     <li>{@link io.jdbd.postgre.stmt.BindStmt}</li>
     *     <li>{@link io.jdbd.postgre.stmt.BindBatchStmt} and {@link BindBatchStmt#getGroupList()} size is one.</li>
     * </ul>
     * so {@link ExtendedStmtTask#getStmt()} from {@link io.jdbd.statement.BindStatement} not {@link io.jdbd.statement.PreparedStatement}.
     * </p>
     *
     * @return true : {@link ExtendedStmtTask#getStmt()} is one shot.
     */
    boolean isOneShot();

    boolean supportFetch();

    boolean needClose();

    @Nullable
    CachePrepare getCache();

    int getFetchSize();

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
