package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.stmt.BatchBindStmt;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.stmt.BindableStmt;
import io.jdbd.mysql.stmt.StmtWrappers;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.*;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.result.*;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.StmtWrapper;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * below is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
 */
final class ComQueryTask extends MySQLCommandTask {

    /*################################## blow StaticStatement underlying api method ##################################*/

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     *
     * @see #ComQueryTask(StmtWrapper, MonoSink, MySQLTaskAdjutant)
     * @see ClientCommandProtocol#update(StmtWrapper)
     */
    static Mono<ResultStatus> update(final StmtWrapper stmt, final MySQLTaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of below methods:
     * <ul>
     *     <li>{@link StaticStatement#executeQuery(String)}</li>
     *     <li>{@link StaticStatement#executeQuery(String, Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see #ComQueryTask(StmtWrapper, FluxSink, MySQLTaskAdjutant)
     * @see ClientCommandProtocol#query(StmtWrapper)
     */
    static Flux<ResultRow> query(final StmtWrapper stmt, MySQLTaskAdjutant adjutant) {
        return Flux.create(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatch(List)} method.
     * </p>
     *
     * @see #ComQueryTask(List, FluxSink, MySQLTaskAdjutant)
     * @see ClientCommandProtocol#batchUpdate(List)
     */
    static Flux<ResultStatus> batchUpdate(final List<StmtWrapper> stmtList, final MySQLTaskAdjutant adjutant) {
        final Flux<ResultStatus> flux;
        if (stmtList.isEmpty()) {
            flux = Flux.error(MySQLExceptions.createEmptySqlException());
        } else {
            flux = Flux.create(sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(stmtList, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }

            });
        }
        return flux;
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsMulti(List)} method.
     * </p>
     *
     * @see ClientCommandProtocol#executeAsMulti(List)
     * @see #ComQueryTask(List, MultiResultSink, MySQLTaskAdjutant)
     */
    static ReactorMultiResult asMulti(List<StmtWrapper> stmtList, final MySQLTaskAdjutant adjutant) {
        final ReactorMultiResult result;
        if (stmtList.isEmpty()) {
            result = JdbdMultiResults.error(MySQLExceptions.createEmptySqlException());
        } else {
            result = JdbdMultiResults.create(adjutant, sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(stmtList, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }
            });
        }
        return result;
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(List)} method.
     * </p>
     *
     * @see ClientCommandProtocol#executeAsFlux(List)
     * @see #ComQueryTask(List, MultiResultSink, MySQLTaskAdjutant)
     */
    static Flux<SingleResult> asFlux(List<StmtWrapper> stmtList, final MySQLTaskAdjutant adjutant) {
        final Flux<SingleResult> flux;
        if (stmtList.isEmpty()) {
            flux = Flux.error(MySQLExceptions.createEmptySqlException());
        } else {
            flux = JdbdMultiResults.createAsFlux(adjutant, sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(stmtList, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }
            });
        }
        return flux;
    }


    /*################################## blow BindableStatement underlying api method ##################################*/

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeUpdate()} method.
     * </p>
     *
     * @see #ComQueryTask(MonoSink, BindableStmt, MySQLTaskAdjutant)
     * @see ComPreparedTask#update(ParamStmt, MySQLTaskAdjutant)
     * @see ClientCommandProtocol#bindableUpdate(BindableStmt)
     */
    static Mono<ResultStatus> bindableUpdate(final BindableStmt wrapper, final MySQLTaskAdjutant adjutant) {
        Mono<ResultStatus> mono;
        Properties<PropertyKey> properties = adjutant.obtainHostInfo().getProperties();
        if (properties.getOrDefault(PropertyKey.useServerPrepStmts, Boolean.class)
                || BindUtils.hasLongData(wrapper.getParamGroup())) {
            // has long data ,can't use client prepare statement.
            mono = ComPreparedTask.update(wrapper, adjutant);
        } else {
            mono = Mono.create(sink -> {
                ComQueryTask task;
                try {
                    task = new ComQueryTask(sink, wrapper, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }

            });
        }
        return mono;
    }

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeBatch()} method.
     * </p>
     *
     * @see #ComQueryTask(FluxSink, BatchBindStmt, MySQLTaskAdjutant)
     * @see ClientCommandProtocol#bindableBatch(BatchBindStmt)
     */
    static Flux<ResultStatus> bindableBatch(final BatchBindStmt stmt, final MySQLTaskAdjutant adjutant) {
        final List<List<BindValue>> parameterGroupList = stmt.getGroupList();
        Properties<PropertyKey> properties = adjutant.obtainHostInfo().getProperties();
        final Flux<ResultStatus> flux;
        if (parameterGroupList.size() > 1000 //TODO decide max size by PropertyKey.maxAllowedPacket
                || properties.getOrDefault(PropertyKey.useServerPrepStmts, Boolean.class)
                || BindUtils.hasLongDataGroup(parameterGroupList)) {
            // has long data ,can't use client prepare statement.
            flux = ComPreparedTask.batchUpdate(stmt, adjutant);
        } else {
            flux = Flux.create(sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(sink, stmt, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }
            });
        }

        return flux;
    }

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindableStatement#executeQuery()}</li>
     *     <li>{@link BindableStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see #ComQueryTask(FluxSink, BindableStmt, MySQLTaskAdjutant)
     * @see ClientCommandProtocol#bindableQuery(BindableStmt)
     */
    static Flux<ResultRow> bindableQuery(final BindableStmt stmt, final MySQLTaskAdjutant adjutant) {
        final Flux<ResultRow> flux;
        Properties<PropertyKey> properties = adjutant.obtainHostInfo().getProperties();
        if (properties.getOrDefault(PropertyKey.useServerPrepStmts, Boolean.class)
                || BindUtils.hasLongData(stmt.getParamGroup())) {
            // has long data ,can't use client prepare statement.
            flux = ComPreparedTask.query(stmt, adjutant);
        } else {
            flux = Flux.create(sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(sink, stmt, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }
            });
        }

        return flux;
    }

    /**
     * <p>
     * This method is one of underlying api of below methods {@link BindableStatement#executeAsMulti()}.
     * </p>
     *
     * @see #ComQueryTask(MultiResultSink, BatchBindStmt, MySQLTaskAdjutant)
     */
    static ReactorMultiResult bindableAsMulti(final BatchBindStmt stmt, final MySQLTaskAdjutant adjutant) {
        final ReactorMultiResult result;
        if (BindUtils.usePrepare(stmt, adjutant)) {
            result = ComPreparedTask.asMulti(stmt, adjutant);
        } else {
            result = JdbdMultiResults.create(adjutant, sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(sink, stmt, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }
            });
        }
        return result;
    }

    /**
     * <p>
     * This method is one of underlying api of below methods {@link BindableStatement#executeAsFlux()}.
     * </p>
     *
     * @see #ComQueryTask(MultiResultSink, BatchBindStmt, MySQLTaskAdjutant)
     */
    static Flux<SingleResult> bindableAsFlux(final BatchBindStmt stmt, final MySQLTaskAdjutant adjutant) {
        final Flux<SingleResult> flux;
        if (BindUtils.usePrepare(stmt, adjutant)) {
            flux = ComPreparedTask.asFlux(stmt, adjutant);
        } else {
            flux = JdbdMultiResults.createAsFlux(adjutant, sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(sink, stmt, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }
            });
        }
        return flux;
    }


    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeAsMulti()} method.
     * </p>
     *
     * @see #ComQueryTask(List, io.jdbd.vendor.result.MultiResultSink, MySQLTaskAdjutant)
     * @see ClientCommandProtocol#bindableMultiStmt(List)
     */
    static ReactorMultiResult bindableMultiStmt(final List<BindableStmt> bindableStmtList
            , final MySQLTaskAdjutant adjutant) {
        final ReactorMultiResult multiResults;
        if (bindableStmtList.isEmpty()) {
            multiResults = JdbdMultiResults.error(MySQLExceptions.createEmptySqlException());
        } else if (Capabilities.supportMultiStatement(adjutant.obtainNegotiatedCapability())) {
            multiResults = JdbdMultiResults.create(adjutant, sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(bindableStmtList, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }

            });
        } else {
            multiResults = JdbdMultiResults.error(MySQLExceptions.notSupportMultiStatementException());
        }
        return multiResults;
    }


    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTask.class);

    private final DownstreamSink downstreamSink;

    private final Mode mode;

    private final int sqlCount;

    private TempMultiStmtStatus tempMultiStmtStatus;

    /**
     * {@link #updateLastResultStates(int, ResultStatus)} can update this filed.
     */
    private int currentResultSequenceId = 1;

    private Phase phase;

    private Pair<Integer, ResultStatus> lastResultStates;

    private List<JdbdException> errorList;

    private ResultSetReader dirtyResultSetReader;

    /**
     * <p>
     * This constructor create instance for {@link #update(StmtWrapper, MySQLTaskAdjutant)}
     * </p>
     * <p>
     * The rule of {@link StaticStatement} underlying api constructor.
     *     <ul>
     *         <li>param 1 : stmt</li>
     *         <li>param 2 : sink</li>
     *         <li>param 3 : adjutant</li>
     *     </ul>
     * </p>
     *
     * @see #update(StmtWrapper, MySQLTaskAdjutant)
     */
    private ComQueryTask(final StmtWrapper stmt, MonoSink<ResultStatus> sink, MySQLTaskAdjutant adjutant)
            throws SQLException {
        super(adjutant);
        this.sqlCount = 1;
        this.mode = Mode.SINGLE_STMT;
        this.packetPublisher = ComQueryCommandWriter.createStaticSingleCommand(stmt, this::addAndGetSequenceId
                , adjutant);
        this.downstreamSink = new UpdateDownstreamSink(this, sink);
    }

    /**
     * <p>
     * This constructor create instance for {@link #query(StmtWrapper, MySQLTaskAdjutant)}
     * </p>
     * <p>
     * The rule of {@link StaticStatement} underlying api constructor.
     *     <ul>
     *         <li>param 1 : stmt</li>
     *         <li>param 2 : sink</li>
     *         <li>param 3 : adjutant</li>
     *     </ul>
     * </p>
     *
     * @see #query(StmtWrapper, MySQLTaskAdjutant)
     */
    private ComQueryTask(final StmtWrapper stmt, FluxSink<ResultRow> sink, MySQLTaskAdjutant adjutant)
            throws SQLException {
        super(adjutant);
        this.sqlCount = 1;
        this.mode = Mode.SINGLE_STMT;
        this.packetPublisher = ComQueryCommandWriter.createStaticSingleCommand(stmt, this::addAndGetSequenceId
                , adjutant);

        this.downstreamSink = new QueryDownstreamSink(this, sink, stmt);
    }

    /**
     * <p>
     * This constructor create instance for {@link #batchUpdate(List, MySQLTaskAdjutant)}
     * </p>
     * <p>
     * The rule of {@link StaticStatement} underlying api constructor.
     *     <ul>
     *         <li>param 1 : stmt</li>
     *         <li>param 2 : sink</li>
     *         <li>param 3 : adjutant</li>
     *     </ul>
     * </p>
     *
     * @see #batchUpdate(List, MySQLTaskAdjutant)
     */
    private ComQueryTask(final List<StmtWrapper> stmtList, final FluxSink<ResultStatus> sink
            , MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);
        this.sqlCount = stmtList.size();

        if (Capabilities.supportMultiStatement(this.negotiatedCapability)) {
            this.mode = Mode.MULTI_STMT;
            this.packetPublisher = ComQueryCommandWriter.createStaticMultiCommand(stmtList, this::addAndGetSequenceId
                    , adjutant);
            this.downstreamSink = new MultiModeBatchUpdateSink(this, sink);
        } else if (this.sqlCount > 3) {
            this.mode = Mode.TEMP_MULTI;
            this.packetPublisher = ComQueryCommandWriter.createStaticMultiCommand(stmtList, this::addAndGetSequenceId
                    , adjutant);
            this.downstreamSink = new MultiModeBatchUpdateSink(this, sink);
        } else {
            this.mode = Mode.SINGLE_STMT;
            this.packetPublisher = ComQueryCommandWriter.createStaticSingleCommand(stmtList.get(0)
                    , this::addAndGetSequenceId, adjutant);
            this.downstreamSink = new SingleModeBatchUpdateSink(this, stmtList, sink);
        }

    }

    /**
     * <p>
     * This constructor create instance for :
     *     <ul>
     *         <li>{@link #asMulti(List, MySQLTaskAdjutant)}</li>
     *         <li>{@link #asFlux(List, MySQLTaskAdjutant)}</li>
     *     </ul>
     * </p>
     * <p>
     * The rule of {@link StaticStatement} underlying api constructor.
     *     <ul>
     *         <li>param 1 : stmt</li>
     *         <li>param 2 : sink</li>
     *         <li>param 3 : adjutant</li>
     *     </ul>
     * </p>
     *
     * @see #asMulti(List, MySQLTaskAdjutant)
     * @see #asFlux(List, MySQLTaskAdjutant)
     */
    private ComQueryTask(final List<StmtWrapper> stmtList, final MultiResultSink sink
            , MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);

        this.sqlCount = stmtList.size();
        if (Capabilities.supportMultiStatement(this.negotiatedCapability)) {
            this.mode = Mode.MULTI_STMT;
            this.packetPublisher = ComQueryCommandWriter.createStaticMultiCommand(stmtList, this::addAndGetSequenceId
                    , adjutant);
            this.downstreamSink = new MultiResultDownstreamSink(this, sink);
        } else if (this.sqlCount > 3) {
            this.mode = Mode.TEMP_MULTI;
            this.packetPublisher = ComQueryCommandWriter.createStaticMultiCommand(stmtList, this::addAndGetSequenceId
                    , adjutant);
            this.downstreamSink = new MultiResultDownstreamSink(this, sink);
        } else {
            this.mode = Mode.SINGLE_STMT;
            this.packetPublisher = ComQueryCommandWriter.createStaticSingleCommand(stmtList.get(0)
                    , this::addAndGetSequenceId, adjutant);
            this.downstreamSink = new SingleModeBatchMultiResultSink(this, stmtList, sink);
        }

    }


    /**
     * <p>
     * This constructor create instance for {@link #bindableUpdate(BindableStmt, MySQLTaskAdjutant)}.
     * </p>
     * <p>
     * The rule of {@link BindableStatement} underlying api constructor.
     *     <ul>
     *         <li>param 1 : sink</li>
     *         <li>param 2 : stmt</li>
     *         <li>param 3 : adjutant</li>
     *     </ul>
     * </p>
     *
     * @see #bindableUpdate(BindableStmt, MySQLTaskAdjutant)
     */
    private ComQueryTask(final MonoSink<ResultStatus> sink, final BindableStmt stmt
            , MySQLTaskAdjutant adjutant) throws SQLException, LongDataReadException {
        super(adjutant);
        this.sqlCount = 1;
        this.mode = Mode.SINGLE_STMT;
        final Iterable<ByteBuf> packetList = ComQueryCommandWriter.createBindableCommand(
                stmt, this::addAndGetSequenceId, adjutant);
        this.packetPublisher = Flux.fromIterable(packetList);
        this.downstreamSink = new UpdateDownstreamSink(this, sink);

    }

    /**
     * <p>
     * This constructor create instance for {@link #bindableQuery(BindableStmt, MySQLTaskAdjutant)}.
     * </p>
     * <p>
     * The rule of {@link BindableStatement} underlying api constructor.
     *     <ul>
     *         <li>param 1 : sink</li>
     *         <li>param 2 : stmt</li>
     *         <li>param 3 : adjutant</li>
     *     </ul>
     * </p>
     *
     * @see #bindableQuery(BindableStmt, MySQLTaskAdjutant)
     */
    private ComQueryTask(final FluxSink<ResultRow> sink, final BindableStmt stmt
            , final MySQLTaskAdjutant adjutant) throws SQLException, LongDataReadException {
        super(adjutant);
        this.sqlCount = 1;
        this.mode = Mode.SINGLE_STMT;
        final Iterable<ByteBuf> packetList = ComQueryCommandWriter.createBindableCommand(
                stmt, this::addAndGetSequenceId, adjutant);
        this.packetPublisher = Flux.fromIterable(packetList);
        this.downstreamSink = new QueryDownstreamSink(this, sink, stmt);
    }

    /**
     * <p>
     * This constructor create instance for {@link #bindableBatch(BatchBindStmt, MySQLTaskAdjutant)}.
     * </p>
     * <p>
     * The rule of {@link BindableStatement} underlying api constructor.
     *     <ul>
     *         <li>param 1 : sink</li>
     *         <li>param 2 : stmt</li>
     *         <li>param 3 : adjutant</li>
     *     </ul>
     * </p>
     *
     * @see #bindableBatch(BatchBindStmt, MySQLTaskAdjutant)
     */
    private ComQueryTask(final FluxSink<ResultStatus> sink, final BatchBindStmt stmt
            , final MySQLTaskAdjutant adjutant) throws SQLException, LongDataReadException {
        super(adjutant);

        final List<List<BindValue>> parameterGroupList = stmt.getGroupList();
        this.sqlCount = parameterGroupList.size();

        final Iterable<ByteBuf> packetList;
        if (Capabilities.supportMultiStatement(this.negotiatedCapability)) {
            this.mode = Mode.MULTI_STMT;
            packetList = ComQueryCommandWriter.createBindableBatchCommand(stmt, this::addAndGetSequenceId, adjutant);
            this.downstreamSink = new MultiModeBatchUpdateSink(this, sink);
        } else if (this.sqlCount > 3) {
            this.mode = Mode.TEMP_MULTI;
            packetList = ComQueryCommandWriter.createBindableBatchCommand(stmt, this::addAndGetSequenceId, adjutant);
            this.downstreamSink = new MultiModeBatchUpdateSink(this, sink);
        } else {
            this.mode = Mode.SINGLE_STMT;
            BindableStmt bindableStmt = StmtWrappers.multi(stmt.getSql(), parameterGroupList.get(0));
            packetList = ComQueryCommandWriter.createBindableCommand(bindableStmt, this::addAndGetSequenceId
                    , adjutant);
            this.downstreamSink = new BindableSingleModeBatchUpdateSink(this, stmt, sink);
        }

        this.packetPublisher = Flux.fromIterable(packetList);
    }


    /**
     * <p>
     * This constructor create instance for :
     * <ul>
     *     <li>{@link #bindableAsMulti(BatchBindStmt, MySQLTaskAdjutant)}</li>
     *     <li>{@link #bindableAsFlux(BatchBindStmt, MySQLTaskAdjutant)}</li>
     * </ul>
     * </p>
     * <p>
     * The rule of {@link BindableStatement} underlying api constructor.
     *     <ul>
     *         <li>param 1 : sink</li>
     *         <li>param 2 : stmt</li>
     *         <li>param 3 : adjutant</li>
     *     </ul>
     * </p>
     *
     * @see #bindableAsMulti(BatchBindStmt, MySQLTaskAdjutant)
     * @see #bindableAsFlux(BatchBindStmt, MySQLTaskAdjutant)
     */
    private ComQueryTask(final MultiResultSink sink, final BatchBindStmt stmt, final MySQLTaskAdjutant adjutant)
            throws SQLException, LongDataReadException {
        super(adjutant);

        final List<List<BindValue>> groupList = stmt.getGroupList();
        this.sqlCount = groupList.size();

        final Iterable<ByteBuf> packetList;
        if (Capabilities.supportMultiStatement(this.negotiatedCapability)) {
            this.mode = Mode.MULTI_STMT;
            packetList = ComQueryCommandWriter.createBindableBatchCommand(stmt, this::addAndGetSequenceId, adjutant);
            this.downstreamSink = new MultiResultDownstreamSink(this, sink);
        } else if (this.sqlCount > 3) {
            this.mode = Mode.TEMP_MULTI;
            packetList = ComQueryCommandWriter.createBindableBatchCommand(stmt, this::addAndGetSequenceId, adjutant);
            this.downstreamSink = new MultiResultDownstreamSink(this, sink);
        } else {
            this.mode = Mode.SINGLE_STMT;
            BindableStmt bindableStmt = StmtWrappers.multi(stmt.getSql(), groupList.get(0));
            packetList = ComQueryCommandWriter.createBindableCommand(bindableStmt, this::addAndGetSequenceId
                    , adjutant);
            this.downstreamSink = new SingleModeBatchBindMultiResultSink(this, sink, stmt);
        }

        this.packetPublisher = Flux.fromIterable(packetList);

    }



    /*################################## blow package template method ##################################*/

    @Override
    protected Publisher<ByteBuf> start() {
        final Publisher<ByteBuf> publisher;
        if (this.mode == Mode.TEMP_MULTI) {
            this.phase = Phase.READ_MULTI_STMT_ENABLE_RESULT;
            publisher = Mono.just(createSetOptionPacket(true));
        } else {
            this.phase = Phase.READ_RESPONSE_RESULT_SET;
            publisher = Objects.requireNonNull(this.packetPublisher, "this.packetPublisher");
            this.packetPublisher = null;
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("send COM_QUERY packet with mode[{}],downstream[{}]", this.mode, this.downstreamSink);
        }
        return publisher;
    }


    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        boolean taskEnd = false;
        boolean continueRead = true;
        while (continueRead) {
            switch (this.phase) {
                case READ_RESPONSE_RESULT_SET: {
                    taskEnd = readResponseResultSet(cumulateBuffer, serverStatusConsumer);
                    continueRead = !taskEnd && PacketUtils.hasOnePacket(cumulateBuffer);
                }
                break;
                case READ_TEXT_RESULT_SET: {
                    taskEnd = readTextResultSet(cumulateBuffer, serverStatusConsumer);
                    continueRead = !taskEnd && PacketUtils.hasOnePacket(cumulateBuffer);
                }
                break;
                case READ_MULTI_STMT_ENABLE_RESULT: {
                    taskEnd = readEnableMultiStmtResponse(cumulateBuffer, serverStatusConsumer);
                    if (!taskEnd) {
                        this.phase = Phase.READ_RESPONSE_RESULT_SET;
                    }
                    continueRead = false;
                }
                break;
                case READ_MULTI_STMT_DISABLE_RESULT: {
                    readDisableMultiStmtResponse(cumulateBuffer, serverStatusConsumer);
                    taskEnd = true;
                    continueRead = false;
                }
                break;
                case LOCAL_INFILE_REQUEST: {
                    throw new IllegalStateException(String.format("%s phase[%s] error.", this, this.phase));
                }
                default:
                    throw MySQLExceptions.createUnknownEnumException(this.phase);
            }
        }
        if (taskEnd) {
            if (this.mode == Mode.TEMP_MULTI && this.tempMultiStmtStatus == TempMultiStmtStatus.ENABLE_SUCCESS) {
                taskEnd = false;
                this.phase = Phase.READ_MULTI_STMT_DISABLE_RESULT;
                this.packetPublisher = Mono.just(createSetOptionPacket(false));
            }
        }
        if (taskEnd) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("COM_QUERY instant[{}] task end.", this.hashCode());
            }
            this.phase = Phase.TASK_EN;
            if (hasError()) {
                this.downstreamSink.error(createException());
            } else {
                this.downstreamSink.complete();
            }
        }
        return taskEnd;
    }

    @Override
    protected Action onError(Throwable e) {
        if (this.phase == Phase.TASK_EN) {
            LOG.error("Unknown error.", e);
        } else {
            this.phase = Phase.TASK_EN;
            addError(MySQLExceptions.wrap(e));
            this.downstreamSink.error(createException());
        }
        return Action.TASK_END;
    }


    /*################################## blow private method ##################################*/

    /**
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean readEnableMultiStmtResponse(final ByteBuf cumulateBuffer
            , final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_MULTI_STMT_ENABLE_RESULT);

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        cumulateBuffer.skipBytes(1); // skip sequence id

        final int status = PacketUtils.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex());
        boolean taskEnd;
        switch (status) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("COM_SET_OPTION enable failure,{}", error);
                }
                // release ByteBuf
                Flux.from(Objects.requireNonNull(this.packetPublisher, "this.packetPublisher"))
                        .map(ByteBuf::release)
                        .subscribe();
                this.packetPublisher = null;
                this.tempMultiStmtStatus = TempMultiStmtStatus.ENABLE_FAILURE;
                addError(MySQLExceptions.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case EofPacket.EOF_HEADER:
            case OkPacket.OK_HEADER: {
                OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok.getStatusFags());
                this.tempMultiStmtStatus = TempMultiStmtStatus.ENABLE_SUCCESS;
                taskEnd = false;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("COM_SET_OPTION enable success.");
                }
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("COM_SET_OPTION response status[%s] error.", status);
        }
        return taskEnd;
    }


    /**
     * @see #decode(ByteBuf, Consumer)
     */
    private void readDisableMultiStmtResponse(final ByteBuf cumulateBuffer
            , final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_MULTI_STMT_DISABLE_RESULT);

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        cumulateBuffer.skipBytes(1); // skip sequence_id

        final int status = PacketUtils.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex());
        switch (status) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("COM_SET_OPTION disabled failure,{}", error);
                }
                this.tempMultiStmtStatus = TempMultiStmtStatus.DISABLE_FAILURE;
                addError(MySQLExceptions.createErrorPacketException(error));
            }
            break;
            case EofPacket.EOF_HEADER:
            case OkPacket.OK_HEADER: {
                OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok.getStatusFags());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("COM_SET_OPTION disabled success.");
                }
                this.tempMultiStmtStatus = TempMultiStmtStatus.DISABLE_SUCCESS;
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("COM_SET_OPTION response status[%s] error.", status);
        }
    }

    /**
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    private boolean readResponseResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_RESPONSE_RESULT_SET);

        final ComQueryResponse response = detectComQueryResponseType(cumulateBuffer, this.negotiatedCapability);
        boolean taskEnd = false;
        switch (response) {
            case ERROR: {
                final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer)); //  sequence_id
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                addErrorForSqlError(error);
                taskEnd = true;
            }
            break;
            case OK: {
                final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));
                OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);

                serverStatusConsumer.accept(ok.getStatusFags());
                // emit update result.
                taskEnd = this.downstreamSink.nextUpdate(MySQLResultStatus.from(ok));
            }
            break;
            case LOCAL_INFILE_REQUEST: {
                this.phase = Phase.LOCAL_INFILE_REQUEST;
                sendLocalFile(cumulateBuffer);
                this.phase = Phase.READ_RESPONSE_RESULT_SET;
            }
            break;
            case TEXT_RESULT: {
                this.phase = Phase.READ_TEXT_RESULT_SET;
                taskEnd = this.downstreamSink.readResultSet(cumulateBuffer, serverStatusConsumer);
            }
            break;
            default:
                throw MySQLExceptions.createUnknownEnumException(response);
        }
        return taskEnd;
    }



    /**
     * @see #start()
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_set_option.html">Protocol::COM_SET_OPTION</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a53f60000da139fc7d547db96635a2c02">enum_mysql_set_option</a>
     */
    private ByteBuf createSetOptionPacket(final boolean enable) {
        ByteBuf packet = this.adjutant.allocator().buffer(7);
        PacketUtils.writeInt3(packet, 3);
        packet.writeByte(0);//use 0 sequenceId

        packet.writeByte(PacketUtils.COM_SET_OPTION);
        //MYSQL_OPTION_MULTI_STATEMENTS_ON : 0
        //MYSQL_OPTION_MULTI_STATEMENTS_OFF : 1
        PacketUtils.writeInt2(packet, enable ? 0 : 1);

        return packet;
    }


    private void addError(JdbdException e) {
        //TODO filter same error.
        List<JdbdException> errorList = this.errorList;
        if (errorList == null) {
            errorList = new ArrayList<>();
            this.errorList = errorList;
        }
        errorList.add(e);
    }

    private JdbdException createException() {
        List<JdbdException> errorList = this.errorList;
        if (MySQLCollections.isEmpty(errorList)) {
            throw new IllegalStateException(String.format("%s No error,reject creat exception.", this));
        }
        JdbdException e;
        if (errorList.size() == 1) {
            e = errorList.get(0);
        } else {
            e = new JdbdCompositeException(errorList, "occur multi error");
        }
        return e;
    }

    /**
     * @see #readResponseResultSet(ByteBuf, Consumer)
     */
    private void addErrorForSqlError(ErrorPacket error) {
        List<JdbdException> errorList = this.errorList;
        JdbdException e = null;
        if (errorList != null && errorList.size() == 1) {
            JdbdException first = errorList.get(0);
            if (first instanceof LocalFileException) {
                SQLException sqlError = new SQLException(error.getErrorMessage()
                        , error.getSqlState(), error.getErrorCode(), first);
                LocalFileException fileError = (LocalFileException) first;
                e = new JdbdSQLException(sqlError, "Local file[%s] send failure,have sent %s bytes."
                        , fileError.getLocalFile(), fileError.getSentBytes());

                errorList.remove(0);
            }
        }
        if (e == null) {
            e = MySQLExceptions.createErrorPacketException(error);
        }
        addError(e);
    }

    /**
     * @see #readResponseResultSet(ByteBuf, Consumer)
     */
    private void updateLastResultStates(final int resultSequenceId, final ResultStatus resultStatus) {
        final int currentSequenceId = this.currentResultSequenceId;
        if (currentSequenceId > this.sqlCount) {
            throw new IllegalStateException(String.format("sqlCount[%s] but currentResultSequenceId[%s],state error."
                    , this.sqlCount, currentSequenceId));
        }
        if (resultSequenceId != currentSequenceId) {
            throw new IllegalArgumentException(
                    String.format("currentResultSequenceId[%s] and resultSequenceId[%s] not match."
                            , currentSequenceId, resultSequenceId));
        }

        Pair<Integer, ResultStatus> pair = this.lastResultStates;
        if (pair != null && pair.getFirst() != resultSequenceId - 1) {
            throw new IllegalStateException(String.format(
                    "%s lastResultStates[sequenceId:%s] but expect update to sequenceId:%s ."
                    , this, pair.getFirst(), resultSequenceId));
        }
        this.lastResultStates = new Pair<>(resultSequenceId, resultStatus);
        this.currentResultSequenceId++;
    }

    private boolean hasError() {
        List<JdbdException> list = this.errorList;
        return list != null && list.size() > 0;
    }

    private boolean containException(Class<? extends JdbdException> clazz) {
        List<JdbdException> errorList = this.errorList;
        boolean contain = false;
        if (errorList != null) {
            for (JdbdException e : errorList) {
                if (clazz.isInstance(e)) {
                    contain = true;
                    break;
                }
            }
        }
        return contain;
    }

    private void replaceIfNeed(Function<JdbdException, JdbdException> function) {
        final List<JdbdException> errorList = this.errorList;
        if (errorList != null) {
            final int size = errorList.size();
            JdbdException temp;
            for (int i = 0; i < size; i++) {
                temp = function.apply(errorList.get(i));
                if (temp != null) {
                    errorList.set(i, temp);
                    break;
                }
            }
        }


    }

    private void addMultiStatementException() {
        if (!hasError()) {
            addError(MySQLExceptions.createMultiStatementException());
        }
    }

    private boolean hasMoreResults() {
        Pair<Integer, ResultStatus> pair = this.lastResultStates;
        return pair != null && pair.getSecond().hasMoreResults();
    }


    /**
     * @see #readResponseResultSet(ByteBuf, Consumer)
     */
    private void sendLocalFile(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.LOCAL_INFILE_REQUEST);

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));
        if (PacketUtils.readInt1AsInt(cumulateBuffer) != PacketUtils.LOCAL_INFILE) {
            throw new IllegalStateException(String.format("%s invoke sendLocalFile method error.", this));
        }
        String localFilePath;
        localFilePath = PacketUtils.readStringFixed(cumulateBuffer, payloadLength - 1
                , this.adjutant.obtainCharsetClient());

        final Path filePath = Paths.get(localFilePath);

        Publisher<ByteBuf> publisher = null;
        if (Files.exists(filePath)) {
            if (Files.isDirectory(filePath)) {
                addError(new LocalFileException(filePath, "Local file[%s] isn directory.", filePath));
            } else if (Files.isReadable(filePath)) {
                try {
                    if (Files.size(filePath) > 0L) {
                        publisher = Flux.create(sink -> doSendLocalFile(sink, filePath));
                    }
                } catch (IOException e) {
                    addError(new LocalFileException(e, filePath, 0L, "Local file[%s] isn't readable.", filePath));
                }

            } else {
                addError(new LocalFileException(filePath, "Local file[%s] isn't readable.", filePath));
            }
        } else {
            addError(new LocalFileException(filePath, "Local file[%s] not exits.", filePath));

        }
        if (publisher == null) {
            publisher = Mono.just(createEmptyPacket());
        }
        this.packetPublisher = publisher;
    }


    /**
     * @see #sendLocalFile(ByteBuf)
     */
    private void doSendLocalFile(final FluxSink<ByteBuf> sink, final Path localPath) {
        long sentBytes = 0L;
        ByteBuf packet = null;
        try (Reader reader = Files.newBufferedReader(localPath, StandardCharsets.UTF_8)) {
            final Charset clientCharset = this.adjutant.obtainCharsetClient();
            final CharBuffer charBuffer = CharBuffer.allocate(1024);
            ByteBuffer byteBuffer;

            // use single packet send local file.
            final int maxPacket = PacketUtils.MAX_PACKET - 1;

            packet = this.adjutant.createPacketBuffer(2048);
            while (reader.read(charBuffer) > 0) { // 1. read chars
                byteBuffer = clientCharset.encode(charBuffer); // 2.encode
                packet.writeBytes(byteBuffer);                // 3. write bytes
                charBuffer.clear();                           // 4. clear char buffer.

                //5. send single packet(not multi packet).
                if (packet.readableBytes() >= maxPacket) {
                    ByteBuf tempPacket = packet.readRetainedSlice(maxPacket);
                    PacketUtils.writePacketHeader(tempPacket, addAndGetSequenceId());
                    sink.next(tempPacket);
                    sentBytes += (maxPacket - PacketUtils.HEADER_SIZE);

                    tempPacket = this.adjutant.createPacketBuffer(Math.max(2048, packet.readableBytes()));
                    tempPacket.writeBytes(packet);
                    packet.release();
                    packet = tempPacket;
                }
            }

            if (packet.readableBytes() == PacketUtils.HEADER_SIZE) {
                sink.next(packet); // send empty packet, tell server file end.
            } else {
                PacketUtils.writePacketHeader(packet, addAndGetSequenceId());
                sink.next(packet);
                sentBytes += (packet.readableBytes() - PacketUtils.HEADER_SIZE);

                sink.next(createEmptyPacket());
            }
        } catch (Throwable e) {
            if (packet != null) {
                packet.release();
            }
            addError(new LocalFileException(e, localPath, sentBytes, "Local file[%s] send failure,sent %s bytes."
                    , localPath, sentBytes));
            sink.next(createEmptyPacket());
        } finally {
            sink.complete();
        }
    }

    /**
     * @see #sendLocalFile(ByteBuf)
     * @see #doSendLocalFile(FluxSink, Path)
     */
    private ByteBuf createEmptyPacket() {
        ByteBuf packet = this.adjutant.allocator().buffer(PacketUtils.HEADER_SIZE);
        PacketUtils.writeInt3(packet, 0);
        packet.writeByte(addAndGetSequenceId());
        return packet;
    }

    /**
     * @see SingleModeBatchUpdateSink#internalNextUpdate(ResultStatus)
     */
    private void sendStaticCommand(final StmtWrapper stmt) throws SQLException {
        // result sequence_id
        this.updateSequenceId(-1);
        this.packetPublisher = ComQueryCommandWriter.createStaticSingleCommand(stmt, this::addAndGetSequenceId
                , this.adjutant);
    }

    /**
     * @see BindableSingleModeBatchUpdateSink#internalNextUpdate(ResultStatus)
     */
    private void sendBindableCommand(final String sql, final List<BindValue> paramGroup) throws SQLException {
        // result sequence_id
        this.updateSequenceId(-1);
        BindableStmt bindableStmt = StmtWrappers.multi(sql, paramGroup);
        this.packetPublisher = Flux.fromIterable(
                ComQueryCommandWriter.createBindableCommand(bindableStmt, this::addAndGetSequenceId, this.adjutant)
        );
    }

    /**
     * @return true result set end.
     */
    private boolean skipTextResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        ResultSetReader dirtyResultSetReader = this.dirtyResultSetReader;
        if (dirtyResultSetReader == null) {
            // create a resettable Reader
            dirtyResultSetReader = createSkipResultSetReader();
            this.dirtyResultSetReader = dirtyResultSetReader;
        }
        return dirtyResultSetReader.read(cumulateBuffer, serverStatusConsumer);
    }

    private ResultSetReader createSkipResultSetReader() {
        return ResultSetReaderBuilder.builder()
                .rowSink(createSkipRowSink())
                .adjutant(ComQueryTask.this.adjutant)
                .sequenceIdUpdater(ComQueryTask.this::updateSequenceId)

                .errorConsumer(ComQueryTask.this::addError)
                .resettable(true)
                .build(TextResultSetReader.class);
    }

    private ResultRowSink createSkipRowSink() {
        return new ResultRowSink() {
            @Override
            public void next(ResultRow resultRow) {
                //no-op
            }

            @Override
            public boolean isCancelled() {
                return true;
            }

            @Override
            public void accept(ResultStatus resultStatus) {
                ComQueryTask.this.updateLastResultStates(ComQueryTask.this.currentResultSequenceId, resultStatus);
            }
        };
    }


    private void assertPhase(Phase expect) {
        if (this.phase != expect) {
            throw new IllegalStateException(String.format("%s current phase isn't %s .", this, expect));
        }
    }


    private void assertSingleMode(DownstreamSink sink) {
        if (this.mode != Mode.SINGLE_STMT) {
            throw new IllegalStateException(String.format("Mode[%s] isn't %s,reject create %s instance."
                    , this.mode, Mode.SINGLE_STMT, sink));
        }
    }


    /*################################## blow private static class ##################################*/

    private interface DownstreamSink {

        void error(JdbdException e);

        /**
         * @return true:task end.
         */
        boolean nextUpdate(ResultStatus states);

        /**
         * @return true:task end.
         */
        boolean readResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);

        void complete();

    }


    private static abstract class AbstractDownstreamSink implements DownstreamSink, ResultRowSink {

        final ComQueryTask task;

        private ResultSetReader skipResultSetReader;

        private ResultStatus lastResultStatus;

        AbstractDownstreamSink(ComQueryTask task) {
            this.task = task;
        }

        @Override
        public final boolean nextUpdate(final ResultStatus states) {
            this.lastResultStatus = states;
            return this.internalNextUpdate(states);
        }

        @Override
        public final boolean readResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            if (this.task.phase != Phase.READ_TEXT_RESULT_SET) {
                throw new IllegalStateException(String.format("%s phase[%s] isn't %s."
                        , this.task, this.task.phase, Phase.READ_TEXT_RESULT_SET));
            }
            final boolean taskEnd;
            if (this.lastResultSetEnd() && this.task.hasError()) {
                if (this.skipResultSet(cumulateBuffer, serverStatusConsumer)) {
                    taskEnd = this.lastNoMoreResult();
                } else {
                    taskEnd = false;
                }
            } else {
                taskEnd = this.internalReadResultSet(cumulateBuffer, serverStatusConsumer);
            }
            return taskEnd;
        }

        @Override
        public final void next(ResultRow row) {
            if (this.skipResultSetReader == null) {
                this.internalNext(row);
            }
        }

        @Override
        public final boolean isCancelled() {
            return this.skipResultSetReader != null || this.internalIsCancelled();
        }

        @Override
        public final void accept(ResultStatus states) {
            this.lastResultStatus = states;
            if (this.skipResultSetReader == null) {
                this.internalAccept(states);
            }
        }

        abstract boolean internalNextUpdate(ResultStatus status);

        abstract boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);

        final boolean skipResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            ResultSetReader resultSetReader = this.skipResultSetReader;
            if (resultSetReader == null) {
                resultSetReader = this.createResultSetReader();
                this.skipResultSetReader = resultSetReader;
            }
            final ResultStatus oldResultStatus = this.lastResultStatus;
            final boolean resultSetEnd;
            resultSetEnd = resultSetReader.read(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                if (this.lastResultStatus == oldResultStatus) {
                    this.addResultSetReaderExceptionIfNeed(resultSetReader);
                }
            }
            return resultSetEnd;
        }

        final ResultSetReader createResultSetReader() {
            return ResultSetReaderBuilder.builder()
                    .rowSink(this)
                    .adjutant(this.task.adjutant)
                    .sequenceIdUpdater(this.task::updateSequenceId)

                    .errorConsumer(this.task::addError)
                    .resettable(true)// Text Protocol can support Reader resettable
                    .build(TextResultSetReader.class);
        }

        final boolean lastNoMoreResult() {
            return !Objects.requireNonNull(this.lastResultStatus, "this.lastResultStatus")
                    .hasMoreResults();
        }

        final void addResultSetReaderExceptionIfNeed(ResultSetReader resultSetReader) {
            // here resultSetReader bug.
            final String message = String.format("%s not invoke %s.accept(ResultStates) method."
                    , resultSetReader, ResultRowSink.class.getName());
            if (this.task.containException(MySQLJdbdException.class)) {
                if (LOG.isDebugEnabled()) {
                    LOG.error(message);
                }
            } else {
                this.task.addError(new MySQLJdbdException(message, new IllegalStateException(message)));
            }
        }

        boolean lastResultSetEnd() {
            assertNotSupportSubClass();
            return true;
        }

        void internalNext(ResultRow row) {
            assertNotSupportSubClass();
        }

        boolean internalIsCancelled() {
            assertNotSupportSubClass();
            return true;
        }

        void internalAccept(ResultStatus states) {
            assertNotSupportSubClass();
        }


        private void assertNotSupportSubClass() {
            if (this instanceof QueryDownstreamSink
                    || this instanceof AbstractMultiResultDownstreamSink) {
                throw new IllegalStateException(String.format("%s not override method.", this));
            }
        }

        @Override
        public final String toString() {
            return this.getClass().getSimpleName();
        }


    }


    private static abstract class AbstractBatchUpdateSink extends AbstractDownstreamSink {

        final FluxSink<ResultStatus> sink;

        AbstractBatchUpdateSink(ComQueryTask task, FluxSink<ResultStatus> sink) {
            super(task);
            this.sink = sink;
        }

        @Override
        final boolean lastResultSetEnd() {
            return true; // must return true;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean taskEnd;
            if (this.skipResultSet(cumulateBuffer, serverStatusConsumer)) {
                final boolean noMoreResult = this.lastNoMoreResult();
                taskEnd = noMoreResult;
                if (this.task.containException(SubscribeException.class)) {
                    if (!noMoreResult) {
                        this.task.replaceIfNeed(TaskUtils::replaceAsBatchUpdateMultiError);
                    }
                } else {
                    this.task.addError(TaskUtils.createBatchUpdateQueryError());
                }
            } else {
                taskEnd = false;
            }
            return taskEnd;
        }

        @Override
        final void internalNext(ResultRow row) {
            //no-op
        }

        @Override
        final boolean internalIsCancelled() {
            return true;
        }

        @Override
        final void internalAccept(ResultStatus states) {
            //no-op
        }


    }// AbstractBatchUpdateSink


    private final static class QueryDownstreamSink extends AbstractDownstreamSink {

        private final FluxSink<ResultRow> sink;

        private final Consumer<ResultStatus> statusConsumer;

        private final ResultSetReader resultSetReader;

        // query result states.
        private ResultStatus status;

        /**
         * @see #ComQueryTask(StmtWrapper, FluxSink, MySQLTaskAdjutant)
         */
        private QueryDownstreamSink(final ComQueryTask task, FluxSink<ResultRow> sink, StmtWrapper stmt) {
            super(task);
            task.assertSingleMode(this);
            this.sink = sink;
            this.statusConsumer = stmt.getStatusConsumer();
            this.resultSetReader = this.createResultSetReader();
        }

        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            if (this.task.containException(SubscribeException.class)) {
                this.task.replaceIfNeed(TaskUtils::replaceAsQueryMultiError);
            } else {
                this.task.addError(TaskUtils.createQueryUpdateError());
            }
            return !status.hasMoreResults();  // here ,maybe ,sql is that call stored procedure,skip rest results.
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.status != null;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean taskEnd;
            if (this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
                final ResultStatus states = Objects.requireNonNull(this.status, "this.resultStatus");
                if (states.hasMoreResults()) {
                    // here ,sql is that call stored procedure,skip rest results.
                    taskEnd = false;
                    if (this.task.containException(SubscribeException.class)) {
                        this.task.replaceIfNeed(TaskUtils::replaceAsQueryMultiError);
                    } else {
                        this.task.addError(TaskUtils.createQueryMultiError());
                    }
                } else {
                    taskEnd = true;
                }
            } else {
                taskEnd = false;
            }
            return taskEnd;
        }

        /**
         * @see ResultRowSink#next(ResultRow)
         */
        @Override
        final void internalNext(ResultRow row) {
            this.sink.next(row);
        }

        /**
         * @see ResultRowSink#isCancelled()
         */
        @Override
        final boolean internalIsCancelled() {
            return this.sink.isCancelled();
        }

        /**
         * @see ResultRowSink#accept(ResultStatus)
         */
        @Override
        final void internalAccept(final ResultStatus status) {
            if (this.status != null) {
                throw new IllegalStateException(String.format("%s.ResultStatus isn't null,reject update.", this));
            }
            this.status = status;
        }

        @Override
        public final void error(final JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            try {
                // invoke user ResultStates Consumer.
                this.statusConsumer.accept(Objects.requireNonNull(this.status, "this.status"));
                this.sink.complete();
            } catch (Throwable e) {
                this.sink.error(TaskUtils.createStateConsumerError(e, this.statusConsumer));
            }
        }


    }// QueryDownstreamSink

    private static final class UpdateDownstreamSink extends AbstractDownstreamSink {

        private final MonoSink<ResultStatus> sink;

        // update result status
        private ResultStatus status;

        /**
         * @see #ComQueryTask(StmtWrapper, MonoSink, MySQLTaskAdjutant)
         * @see #ComQueryTask(MonoSink, BindableStmt, MySQLTaskAdjutant)
         */
        private UpdateDownstreamSink(final ComQueryTask task, MonoSink<ResultStatus> sink) {
            super(task);
            task.assertSingleMode(this);
            this.sink = sink;
        }

        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            if (this.status == null) {
                this.status = status;
            }
            final boolean taskEnd;
            if (status.hasMoreResults()) {
                taskEnd = false;
                if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsUpdateMultiError);
                } else {
                    this.task.addError(TaskUtils.createUpdateMultiError());
                }
            } else {
                taskEnd = true;
            }
            return taskEnd;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean taskEnd;
            if (this.skipResultSet(cumulateBuffer, serverStatusConsumer)) {
                taskEnd = this.lastNoMoreResult();
                if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsUpdateMultiError);
                } else {
                    this.task.addError(TaskUtils.createUpdateQueryError());
                }
            } else {
                taskEnd = false;
            }
            return taskEnd;
        }

        @Override
        public final void error(final JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.success(Objects.requireNonNull(this.status, "this.status"));
        }


    }// UpdateDownstreamSink


    /**
     * <p>
     * This static inner class is one of underlying implementation downstream sink of below methods.
     * <ul>
     *     <li>{@link StaticStatement#executeBatch(List)}</li>
     * </ul>
     * only when stmtList size less than 4 ,use this sink , or use {@link MultiModeBatchUpdateSink}
     * </p>
     *
     * @see MultiModeBatchUpdateSink
     */
    private static final class SingleModeBatchUpdateSink extends AbstractBatchUpdateSink {

        private final List<StmtWrapper> stmtList;

        //start from 1 .
        private int index = 1;

        /**
         * @see #ComQueryTask(List, FluxSink, MySQLTaskAdjutant)
         * @see #ComQueryTask(FluxSink, BindableStmt, MySQLTaskAdjutant)
         */
        private SingleModeBatchUpdateSink(final ComQueryTask task, List<StmtWrapper> stmtList
                , FluxSink<ResultStatus> sink) {
            super(task, sink);
            if (task.mode != Mode.SINGLE_STMT) {
                throw new IllegalArgumentException(String.format("%s isn't %s.", task, Mode.SINGLE_STMT));
            }
            if (stmtList.size() > 3) {
                throw new IllegalArgumentException("stmtList size error,please use multi-statement");
            }
            this.stmtList = stmtList;
        }

        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            final boolean taskEnd;
            if (this.task.hasError()) {
                taskEnd = !status.hasMoreResults();
            } else if (status.hasMoreResults()) {
                taskEnd = false;
                if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsBatchUpdateMultiError);
                } else {
                    this.task.addError(TaskUtils.createBatchUpdateMultiError());
                }
            } else {
                this.sink.next(status); // drain to downstream
                final int groupIndex = this.index++;
                if (groupIndex < this.stmtList.size()) {
                    try {
                        this.task.sendStaticCommand(this.stmtList.get(groupIndex));
                    } catch (Throwable e) {
                        this.task.addError(MySQLExceptions.wrap(e));
                    }
                    taskEnd = this.task.hasError();
                } else {
                    taskEnd = true;
                }
            }
            return taskEnd;
        }


        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.complete();
        }


    }// SingleModeBatchUpdateSink

    private static final class BindableSingleModeBatchUpdateSink extends AbstractBatchUpdateSink {

        private final String sql;

        private final List<List<BindValue>> groupList;

        // based zero
        private int index;

        /**
         * @see #ComQueryTask(FluxSink, BatchBindStmt, MySQLTaskAdjutant)
         */
        private BindableSingleModeBatchUpdateSink(final ComQueryTask task, final BatchBindStmt wrapper
                , FluxSink<ResultStatus> sink) {
            super(task, sink);
            this.sql = wrapper.getSql();
            this.groupList = wrapper.getGroupList();

        }

        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            final boolean taskEnd;
            if (this.task.hasError()) {
                taskEnd = !status.hasMoreResults();
            } else if (status.hasMoreResults()) {
                // here block ,sql is that call stored procedure,skip rest results.
                taskEnd = false;
                if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsBatchUpdateMultiError);
                } else {
                    this.task.addError(TaskUtils.createBatchUpdateMultiError());
                }
            } else {
                this.sink.next(status); // drain to downstream
                final int groupIndex = this.index++;
                if (groupIndex < this.groupList.size()) {
                    try {
                        this.task.sendBindableCommand(this.sql, this.groupList.get(groupIndex));
                    } catch (Throwable e) {
                        this.task.addError(MySQLExceptions.wrap(e));
                    }
                    taskEnd = this.task.hasError();
                } else {
                    taskEnd = true;
                }
            }
            return taskEnd;
        }


        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.complete();
        }


    }//BindableSingleModeBatchUpdateSink


    /**
     * <p>
     * This static inner class is one of underlying implementation downstream sink of below methods.
     * <ul>
     *     <li>{@link StaticStatement#executeBatch(List)}</li>
     *     <li>{@link BindableStatement#executeBatch()}</li>
     * </ul>
     * only when stmtList size great than 3 ,use this sink , or use {@link SingleModeBatchUpdateSink}
     * </p>
     *
     * @see SingleModeBatchUpdateSink
     */
    private static final class MultiModeBatchUpdateSink extends AbstractBatchUpdateSink {


        // based zero
        private int resultSequenceId = 0;

        /**
         * @see #ComQueryTask(List, FluxSink, MySQLTaskAdjutant)
         * @see #ComQueryTask(FluxSink, BatchBindStmt, MySQLTaskAdjutant)
         */
        private MultiModeBatchUpdateSink(final ComQueryTask task, FluxSink<ResultStatus> sink) {
            super(task, sink);
            if (task.mode == Mode.SINGLE_STMT) {
                throw new IllegalStateException(String.format("mode[%s] error.", task.mode));
            }
        }

        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            if (this.task.hasError()) {
                if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsBatchUpdateMultiError);
                } else {
                    this.task.addError(TaskUtils.createBatchUpdateMultiError());
                }
            } else {
                final int currentSequenceId = this.resultSequenceId++;
                if (currentSequenceId < this.task.sqlCount) {
                    this.sink.next(status);// drain to downstream
                } else {
                    this.task.addError(TaskUtils.createBatchUpdateMultiError());
                }
            }
            return !status.hasMoreResults();
        }

        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.complete();
        }

    }// MultiModeBatchUpdateSink


    private static abstract class AbstractMultiResultDownstreamSink extends AbstractDownstreamSink {

        final MultiResultSink sink;

        final ResultSetReader resultSetReader;

        // query result status
        ResultStatus status;

        QuerySink querySink;

        AbstractMultiResultDownstreamSink(ComQueryTask task, MultiResultSink sink) {
            super(task);
            this.sink = sink;
            this.resultSetReader = this.createResultSetReader();
        }

        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.complete();
        }


        @Override
        final boolean lastResultSetEnd() {
            return this.querySink == null;
        }

        @Override
        final void internalNext(ResultRow row) {
            final QuerySink querySink = this.querySink;
            if (querySink == null) {
                throw new NullPointerException("this.querySink");
            }
            querySink.next(row);
        }

        @Override
        final boolean internalIsCancelled() {
            final QuerySink querySink = this.querySink;
            if (querySink == null) {
                throw new NullPointerException("this.querySink");
            }
            return querySink.isCancelled();
        }

        @Override
        final void internalAccept(ResultStatus states) {
            if (this.status != null) {
                // here,bug
                throw new IllegalStateException(String.format("%s.status not null.", this));
            }
            this.status = states;
        }


    }

    private static final class MultiResultDownstreamSink extends AbstractMultiResultDownstreamSink {

        /**
         * @see #ComQueryTask(List, MultiResultSink, MySQLTaskAdjutant)
         * @see #ComQueryTask(MultiResultSink, BatchBindStmt, MySQLTaskAdjutant)
         */
        private MultiResultDownstreamSink(final ComQueryTask task, MultiResultSink sink) {
            super(task, sink);
        }

        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            if (!this.task.hasError()) {
                this.sink.nextUpdate(status);
            }
            return !status.hasMoreResults();
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final QuerySink querySink;
            if (this.querySink == null) {
                querySink = this.sink.nextQuery();
                this.querySink = querySink;
            } else {
                querySink = this.querySink;
            }

            final boolean taskEnd;
            if (this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
                final ResultStatus status = Objects.requireNonNull(this.status, "this.status");
                taskEnd = !status.hasMoreResults();
                this.querySink = null; // clear for next query
                this.status = null;// clear for next query

                if (!this.task.hasError()) {
                    querySink.accept(status);
                    querySink.complete();
                }
            } else {
                taskEnd = false;
            }
            return taskEnd;
        }


    }// MultiResultDownstreamSink

    /**
     * <p>
     * This static inner class is one of underlying api downstream sink of below:
     * <ul>
     *     <li>{@link StaticStatement#executeAsMulti(List)}</li>
     *     <li>{@link StaticStatement#executeAsFlux(List)}</li>
     * </ul>
     * </p>
     */
    private static final class SingleModeBatchMultiResultSink extends AbstractMultiResultDownstreamSink {

        private final List<StmtWrapper> stmtList;

        //start from 1 .
        private int index = 1;

        private SingleModeBatchMultiResultSink(final ComQueryTask task, List<StmtWrapper> stmtList
                , MultiResultSink sink) {
            super(task, sink);
            if (task.mode != Mode.SINGLE_STMT) {
                throw new IllegalArgumentException("mode error");
            }
            if (stmtList.size() > 3) {
                throw new IllegalArgumentException("stmtList size error.");
            }
            this.stmtList = stmtList;
        }


        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            final boolean taskEnd;
            if (this.task.hasError()) {
                taskEnd = !status.hasMoreResults();
            } else {
                this.sink.nextUpdate(status);// drain to downstream
                if (status.hasMoreResults()) {
                    taskEnd = false;
                } else {
                    taskEnd = this.sendCommand();
                }
            }
            return taskEnd;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final QuerySink querySink;
            if (this.querySink == null) {
                querySink = this.sink.nextQuery();
                this.querySink = querySink;
            } else {
                querySink = this.querySink;
            }

            final boolean taskEnd;
            if (this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
                final ResultStatus status = Objects.requireNonNull(this.status, "this.status");
                this.querySink = null; // clear for next query
                this.status = null;// clear for next query

                if (this.task.hasError()) {
                    taskEnd = !status.hasMoreResults();
                } else {
                    querySink.accept(status);// drain to downstream
                    querySink.complete();// drain to downstream

                    if (status.hasMoreResults()) {
                        taskEnd = false;
                    } else {
                        taskEnd = this.sendCommand();
                    }
                }
            } else {
                taskEnd = false;
            }
            return taskEnd;
        }

        /**
         * @return true : task end.
         */
        private boolean sendCommand() {
            final boolean taskEnd;
            final int currentIndex = this.index++;
            if (currentIndex < this.stmtList.size()) {
                try {
                    this.task.sendStaticCommand(this.stmtList.get(currentIndex));
                } catch (Throwable e) {
                    this.task.addError(JdbdExceptions.wrap(e));
                }
                taskEnd = this.task.hasError();
            } else {
                taskEnd = true;
            }
            return taskEnd;
        }


    }// SingleModeBatchMultiResultSink


    /**
     * <p>
     * The static inner class is one of underlying api implementation downstream sink of :
     *     <ul>
     *         <li>{@link BindableStatement#executeAsMulti()}</li>
     *         <li>{@link BindableStatement#executeAsFlux()}</li>
     *     </ul>
     * </p>
     */
    private static final class SingleModeBatchBindMultiResultSink extends AbstractMultiResultDownstreamSink {

        private final String sql;

        private final List<List<BindValue>> groupList;

        // start from 1 .
        private int index = 1;

        /**
         * @see #ComQueryTask(MultiResultSink, BatchBindStmt, MySQLTaskAdjutant)
         */
        private SingleModeBatchBindMultiResultSink(final ComQueryTask task, MultiResultSink sink
                , BatchBindStmt wrapper) {
            super(task, sink);
            this.sql = wrapper.getSql();
            this.groupList = wrapper.getGroupList();
        }


        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            final boolean taskEnd;
            if (this.task.hasError()) {
                taskEnd = !status.hasMoreResults();
            } else {
                this.sink.nextUpdate(status); // drain to downstream

                if (status.hasMoreResults()) {
                    taskEnd = false;
                } else {
                    taskEnd = this.sendCommand();
                }
            }
            return taskEnd;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final QuerySink querySink;
            if (this.querySink == null) {
                querySink = this.sink.nextQuery();
                this.querySink = querySink;
            } else {
                querySink = this.querySink;
            }
            final boolean taskEnd;
            if (this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
                final ResultStatus status = Objects.requireNonNull(this.status, "this.status");
                this.querySink = null; // clear for next query
                this.status = null;// clear for next query

                if (this.task.hasError()) {
                    taskEnd = !status.hasMoreResults();
                } else {
                    querySink.accept(status); // drain to downstream
                    querySink.complete(); // drain to downstream

                    if (status.hasMoreResults()) {
                        taskEnd = false;
                    } else {
                        taskEnd = this.sendCommand();
                    }
                }
            } else {
                taskEnd = false;
            }
            return taskEnd;
        }

        /**
         * @return true:task end
         */
        private boolean sendCommand() {
            final boolean taskEnd;
            final int groupIndex = this.index++;
            if (groupIndex < this.groupList.size()) {
                try {
                    this.task.sendBindableCommand(this.sql, this.groupList.get(groupIndex));
                } catch (Throwable e) {
                    this.task.addError(MySQLExceptions.wrap(e));
                }
                taskEnd = this.task.hasError();
            } else {
                taskEnd = true;
            }
            return taskEnd;
        }


    }// SingleModeBatchMultiResultSink


    /**
     * invoke this method after invoke {@link PacketUtils#hasOnePacket(ByteBuf)}.
     *
     * @see #decode(ByteBuf, Consumer)
     */
    static ComQueryResponse detectComQueryResponseType(final ByteBuf cumulateBuffer, final int negotiatedCapability) {
        int readerIndex = cumulateBuffer.readerIndex();
        final int payloadLength = PacketUtils.getInt3(cumulateBuffer, readerIndex);
        // skip header
        readerIndex += PacketUtils.HEADER_SIZE;
        ComQueryResponse responseType;
        final boolean metadata = (negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;

        switch (PacketUtils.getInt1AsInt(cumulateBuffer, readerIndex++)) {
            case 0: {
                if (metadata && PacketUtils.obtainLenEncIntByteCount(cumulateBuffer, readerIndex) + 1 == payloadLength) {
                    responseType = ComQueryResponse.TEXT_RESULT;
                } else {
                    responseType = ComQueryResponse.OK;
                }
            }
            break;
            case ErrorPacket.ERROR_HEADER:
                responseType = ComQueryResponse.ERROR;
                break;
            case PacketUtils.LOCAL_INFILE:
                responseType = ComQueryResponse.LOCAL_INFILE_REQUEST;
                break;
            default:
                responseType = ComQueryResponse.TEXT_RESULT;

        }
        return responseType;
    }

    /*################################## blow private static method ##################################*/


    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    private enum ComQueryResponse {
        OK,
        ERROR,
        TEXT_RESULT,
        LOCAL_INFILE_REQUEST
    }


    private enum Phase {
        READ_RESPONSE_RESULT_SET,
        READ_TEXT_RESULT_SET,
        LOCAL_INFILE_REQUEST,
        READ_MULTI_STMT_ENABLE_RESULT,
        READ_MULTI_STMT_DISABLE_RESULT,
        TASK_EN
    }


    private enum Mode {
        SINGLE_STMT,
        MULTI_STMT,
        TEMP_MULTI
    }

    private enum TempMultiStmtStatus {
        ENABLE_SUCCESS,
        ENABLE_FAILURE,
        DISABLE_SUCCESS,
        DISABLE_FAILURE
    }


}
