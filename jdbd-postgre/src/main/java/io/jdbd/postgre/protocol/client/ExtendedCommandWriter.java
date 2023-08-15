package io.jdbd.postgre.protocol.client;

import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
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
     * One round trip : once network round trip .
     * </p>
     * <p>
     * {@link ExtendedStmtTask#getStmt()} satisfy one of below conditions:
     * <ul>
     *     <li>{@link ParamStmt} and  {@link ParamStmt#getFetchSize()} == 0 </li>
     *     <li> {@link ParamBatchStmt} and {@link ParamBatchStmt#getGroupList()} > 1 or  {@link ParamBatchStmt#getFetchSize()}  == 0</li>
     * </ul>
     * so {@link ExtendedStmtTask#getStmt()} from {@link io.jdbd.statement.BindStatement} not {@link io.jdbd.statement.PreparedStatement}.
     * </p>
     *
     * @return true : {@link ExtendedStmtTask#getStmt()} is one shot.
     */
    boolean isOneRoundTrip();

    boolean isNeedPrepare();

    boolean supportFetch();

    boolean isNeedClose();

    @Nullable
    PostgreStmt getCache();

    int getFetchSize();

    String getReplacedSql();

    String getStatementName();

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
