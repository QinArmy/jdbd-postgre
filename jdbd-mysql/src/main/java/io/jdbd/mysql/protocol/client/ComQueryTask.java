package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.ResultStatusConsumerException;
import io.jdbd.mysql.stmt.BatchBindStmt;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.stmt.BindableStmt;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.*;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.result.*;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.Stmt;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
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
     * @see #ComQueryTask(Stmt, MonoSink, MySQLTaskAdjutant)
     * @see ClientCommandProtocol#update(Stmt)
     */
    static Mono<ResultStatus> update(final Stmt stmt, final MySQLTaskAdjutant adjutant) {
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
     * @see #ComQueryTask(Stmt, FluxSink, MySQLTaskAdjutant)
     * @see ClientCommandProtocol#query(Stmt)
     */
    static Flux<ResultRow> query(final Stmt stmt, final MySQLTaskAdjutant adjutant) {
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
    static Flux<ResultStatus> batchUpdate(final List<Stmt> stmtList, final MySQLTaskAdjutant adjutant) {
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
    static ReactorMultiResult asMulti(List<Stmt> stmtList, final MySQLTaskAdjutant adjutant) {
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
    static Flux<SingleResult> asFlux(List<Stmt> stmtList, final MySQLTaskAdjutant adjutant) {
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
    static Mono<ResultStatus> bindableUpdate(final BindableStmt stmt, final MySQLTaskAdjutant adjutant) {
        final Mono<ResultStatus> mono;
        if (BindUtils.usePrepare(stmt, adjutant)) {
            mono = ComPreparedTask.update(stmt, adjutant);
        } else {
            mono = Mono.create(sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(sink, stmt, adjutant);
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
        if (BindUtils.usePrepare(stmt, adjutant)) {
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
     * This method is one of underlying api of {@link BindableStatement#executeBatch()} method.
     * </p>
     *
     * @see #ComQueryTask(FluxSink, BatchBindStmt, MySQLTaskAdjutant)
     * @see ClientCommandProtocol#bindableBatch(BatchBindStmt)
     */
    static Flux<ResultStatus> bindableBatch(final BatchBindStmt stmt, final MySQLTaskAdjutant adjutant) {
        final Flux<ResultStatus> flux;
        if (BindUtils.useBatchPrepare(stmt, adjutant)) {
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
     * This method is one of underlying api of below methods {@link BindableStatement#executeAsMulti()}.
     * </p>
     *
     * @see #ComQueryTask(MultiResultSink, BatchBindStmt, MySQLTaskAdjutant)
     */
    static ReactorMultiResult bindableAsMulti(final BatchBindStmt stmt, final MySQLTaskAdjutant adjutant) {
        final ReactorMultiResult result;
        if (BindUtils.useBatchPrepare(stmt, adjutant)) {
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
        if (BindUtils.useBatchPrepare(stmt, adjutant)) {
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

    /*################################## blow MultiStatement method ##################################*/


    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeAsMulti()} method.
     * </p>
     *
     * @see #ComQueryTask(MySQLTaskAdjutant, List, MultiResultSink)
     * @see ClientCommandProtocol#multiStmtAsMulti(List)
     */
    static ReactorMultiResult multiStmtAsMulti(final List<BindableStmt> stmtList, final MySQLTaskAdjutant adjutant) {
        final ReactorMultiResult multiResults;
        if (stmtList.isEmpty()) {
            multiResults = JdbdMultiResults.error(MySQLExceptions.createEmptySqlException());
        } else if (Capabilities.supportMultiStatement(adjutant.obtainNegotiatedCapability())) {
            multiResults = JdbdMultiResults.create(adjutant, sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(adjutant, stmtList, sink);
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

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeAsFlux()} method.
     * </p>
     *
     * @see #ComQueryTask(MySQLTaskAdjutant, List, MultiResultSink)
     * @see ClientCommandProtocol#multiStmtAsFlux(List)
     */
    static Flux<SingleResult> multiStmtAsFlux(final List<BindableStmt> stmtList, final MySQLTaskAdjutant adjutant) {
        final Flux<SingleResult> flux;
        if (stmtList.isEmpty()) {
            flux = Flux.error(MySQLExceptions.createEmptySqlException());
        } else if (Capabilities.supportMultiStatement(adjutant.obtainNegotiatedCapability())) {
            flux = JdbdMultiResults.createAsFlux(adjutant, sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(adjutant, stmtList, sink);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrap(e));
                }
            });
        } else {
            flux = Flux.error(MySQLExceptions.notSupportMultiStatementException());
        }
        return flux;
    }


    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTask.class);

    private final DownstreamSink downstreamSink;

    private final Mode mode;

    private final int sqlCount;

    private TempMultiStmtStatus tempMultiStmtStatus;

    private Phase phase;


    private List<JdbdException> errorList;


    /**
     * <p>
     * This constructor create instance for {@link #update(Stmt, MySQLTaskAdjutant)}
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
     * @see #update(Stmt, MySQLTaskAdjutant)
     */
    private ComQueryTask(final Stmt stmt, MonoSink<ResultStatus> sink, MySQLTaskAdjutant adjutant)
            throws SQLException {
        super(adjutant);
        this.sqlCount = 1;
        this.mode = Mode.SINGLE_STMT;
        this.packetPublisher = Flux.fromIterable(
                ComQueryCommandWriter.createStaticSingleCommand(stmt, this::addAndGetSequenceId, adjutant)
        );
        this.downstreamSink = new UpdateDownstreamSink(this, sink);
    }

    /**
     * <p>
     * This constructor create instance for {@link #query(Stmt, MySQLTaskAdjutant)}
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
     * @see #query(Stmt, MySQLTaskAdjutant)
     */
    private ComQueryTask(final Stmt stmt, FluxSink<ResultRow> sink, MySQLTaskAdjutant adjutant)
            throws SQLException {
        super(adjutant);
        this.sqlCount = 1;
        this.mode = Mode.SINGLE_STMT;
        this.packetPublisher = Flux.fromIterable(
                ComQueryCommandWriter.createStaticSingleCommand(stmt, this::addAndGetSequenceId, adjutant)
        );

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
    private ComQueryTask(final List<Stmt> stmtList, final FluxSink<ResultStatus> sink
            , MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);
        this.sqlCount = stmtList.size();

        if (Capabilities.supportMultiStatement(this.negotiatedCapability)) {
            this.mode = Mode.MULTI_STMT;
            this.packetPublisher = Flux.fromIterable(
                    ComQueryCommandWriter.createStaticMultiCommand(stmtList, this::addAndGetSequenceId, adjutant)
            );
            this.downstreamSink = new MultiModeBatchUpdateSink(this, sink);
        } else if (this.sqlCount > 3) {
            this.mode = Mode.TEMP_MULTI;
            this.packetPublisher = Flux.fromIterable(
                    ComQueryCommandWriter.createStaticMultiCommand(stmtList, this::addAndGetSequenceId, adjutant)
            );
            this.downstreamSink = new MultiModeBatchUpdateSink(this, sink);
        } else {
            this.mode = Mode.SINGLE_STMT;
            final Stmt stmt = stmtList.get(0);
            this.packetPublisher = Flux.fromIterable(
                    ComQueryCommandWriter.createStaticSingleCommand(stmt, this::addAndGetSequenceId, adjutant)
            );
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
    private ComQueryTask(final List<Stmt> stmtList, final MultiResultSink sink
            , MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);

        this.sqlCount = stmtList.size();
        if (Capabilities.supportMultiStatement(this.negotiatedCapability)) {
            this.mode = Mode.MULTI_STMT;
            this.packetPublisher = Flux.fromIterable(
                    ComQueryCommandWriter.createStaticMultiCommand(stmtList, this::addAndGetSequenceId, adjutant)
            );
            this.downstreamSink = new MultiResultDownstreamSink(this, sink);
        } else if (this.sqlCount > 3) {
            this.mode = Mode.TEMP_MULTI;
            this.packetPublisher = Flux.fromIterable(
                    ComQueryCommandWriter.createStaticMultiCommand(stmtList, this::addAndGetSequenceId, adjutant)
            );
            this.downstreamSink = new MultiResultDownstreamSink(this, sink);
        } else {
            this.mode = Mode.SINGLE_STMT;
            final Stmt stmt = stmtList.get(0);
            this.packetPublisher = Flux.fromIterable(
                    ComQueryCommandWriter.createStaticSingleCommand(stmt, this::addAndGetSequenceId, adjutant)
            );
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
        this.packetPublisher = Flux.fromIterable(
                ComQueryCommandWriter.createBindableCommand(stmt, this::addAndGetSequenceId, adjutant)
        );
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
        this.packetPublisher = Flux.fromIterable(
                ComQueryCommandWriter.createBindableCommand(stmt, this::addAndGetSequenceId, adjutant)
        );
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
            BindableStmt bindableStmt = Stmts.multi(stmt.getSql(), parameterGroupList.get(0));
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
            BindableStmt bindableStmt = Stmts.multi(stmt.getSql(), groupList.get(0));
            packetList = ComQueryCommandWriter.createBindableCommand(bindableStmt, this::addAndGetSequenceId
                    , adjutant);
            this.downstreamSink = new SingleModeBatchBindMultiResultSink(this, sink, stmt);
        }

        this.packetPublisher = Flux.fromIterable(packetList);

    }

    /**
     * <p>
     * This constructor create instance for :
     * <ul>
     *     <li>{@link #multiStmtAsMulti(List, MySQLTaskAdjutant)}</li>
     *     <li>{@link #multiStmtAsFlux(List, MySQLTaskAdjutant)}</li>
     * </ul>
     * </p>
     * <p>
     * The rule of {@link MultiStatement} underlying api constructor.
     *     <ul>
     *         <li>param 1 : adjutant</li>
     *         <li>param 2 : stmt</li>
     *         <li>param 3 : sink</li>
     *     </ul>
     * </p>
     *
     * @see #multiStmtAsMulti(List, MySQLTaskAdjutant)
     * @see #multiStmtAsFlux(List, MySQLTaskAdjutant)
     */
    private ComQueryTask(final MySQLTaskAdjutant adjutant, List<BindableStmt> stmtList, MultiResultSink sink)
            throws SQLException, LongDataReadException {
        super(adjutant);

        this.sqlCount = stmtList.size();
        this.mode = Mode.MULTI_STMT;
        this.packetPublisher = Flux.fromIterable(
                ComQueryCommandWriter.createBindableMultiCommand(stmtList, this::addAndGetSequenceId, adjutant)
        );
        this.downstreamSink = new MultiResultDownstreamSink(this, sink);

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
                taskEnd = readTextResultSet(cumulateBuffer, serverStatusConsumer);

            }
            break;
            default:
                throw MySQLExceptions.createUnknownEnumException(response);
        }
        return taskEnd;
    }

    /**
     * <p>
     * when text result set end, update {@link #phase}.
     * </p>
     *
     * @return true: task end.
     */
    private boolean readTextResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_TEXT_RESULT_SET);
        final boolean taskEnd;
        if (this.downstreamSink.readResultSet(cumulateBuffer, serverStatusConsumer)) {
            if (this.downstreamSink.hasMoreResult()) {
                this.phase = Phase.READ_RESPONSE_RESULT_SET;
                taskEnd = false;
            } else if (hasError()) {
                taskEnd = true;
            } else if (this.downstreamSink instanceof SingleModeBatchDownstreamSink) {
                final SingleModeBatchDownstreamSink sink = (SingleModeBatchDownstreamSink) this.downstreamSink;
                taskEnd = !sink.hasMoreGroup() || sink.sendCommand();
                if (!taskEnd) {
                    this.phase = Phase.READ_RESPONSE_RESULT_SET;
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
            publisher = Mono.just(PacketUtils.createEmptyPacket(this.adjutant.allocator(), addAndGetSequenceId()));
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
            final int maxPacket = Math.min(PacketUtils.MAX_PACKET - 1
                    , this.adjutant.obtainHostInfo().maxAllowedPayload());

            packet = this.adjutant.createPacketBuffer(2048);
            while (reader.read(charBuffer) > 0) { // 1. read chars
                byteBuffer = clientCharset.encode(charBuffer); // 2.encode
                packet.writeBytes(byteBuffer);                // 3. write bytes
                charBuffer.clear();                           // 4. clear char buffer.

                //5. send single packet(not multi packet).
                while (packet.readableBytes() >= maxPacket) {
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
            PacketUtils.writePacketHeader(packet, addAndGetSequenceId());
            sink.next(packet);
            sentBytes += (packet.readableBytes() - PacketUtils.HEADER_SIZE);
            if (packet.readableBytes() > PacketUtils.HEADER_SIZE) {
                // send empty packet, tell server file end.
                sink.next(PacketUtils.createEmptyPacket(this.adjutant.allocator(), addAndGetSequenceId()));
            }
        } catch (Throwable e) {
            if (packet != null) {
                packet.release();
            }
            addError(new LocalFileException(e, localPath, sentBytes, "Local file[%s] send failure,sent %s bytes."
                    , localPath, sentBytes));
            sink.next(PacketUtils.createEmptyPacket(this.adjutant.allocator(), addAndGetSequenceId()));
        } finally {
            sink.complete();
        }

    }


    /**
     * @see SingleModeBatchUpdateSink#internalNextUpdate(ResultStatus)
     */
    private void sendStaticCommand(final Stmt stmt) throws SQLException {
        // reset sequence_id
        this.updateSequenceId(-1);
        this.packetPublisher = Flux.fromIterable(
                ComQueryCommandWriter.createStaticSingleCommand(stmt, this::addAndGetSequenceId, this.adjutant)
        );
    }

    /**
     * @see BindableSingleModeBatchUpdateSink#internalNextUpdate(ResultStatus)
     */
    private void sendBindableCommand(final String sql, final List<BindValue> paramGroup) throws SQLException {
        // reset sequence_id
        this.updateSequenceId(-1);
        BindableStmt bindableStmt = Stmts.multi(sql, paramGroup);
        this.packetPublisher = Flux.fromIterable(
                ComQueryCommandWriter.createBindableCommand(bindableStmt, this::addAndGetSequenceId, this.adjutant)
        );
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
         * <p>
         * This method maybe invoke below methods:
         *     <ul>
         *         <li>{@link #sendStaticCommand(Stmt)}</li>
         *         <li>{@link #sendBindableCommand(String, List)}</li>
         *     </ul>
         * </p>
         *
         * @return true:task end.
         */
        boolean nextUpdate(ResultStatus states);

        /**
         * <p>
         * This method maybe invoke below methods:
         *     <ul>
         *         <li>{@link #sendStaticCommand(Stmt)}</li>
         *         <li>{@link #sendBindableCommand(String, List)}</li>
         *     </ul>
         * </p>
         *
         * @return true: ResultSet end
         */
        boolean readResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);

        void complete();

        boolean hasMoreResult();

    }

    private interface SingleModeBatchDownstreamSink extends DownstreamSink {

        boolean hasMoreGroup();

        /**
         * @return true: task end
         */
        boolean sendCommand();
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
            final boolean resultSetEnd;
            if (this.task.hasError() && this.lastResultSetEnd()) {
                resultSetEnd = this.skipResultSet(cumulateBuffer, serverStatusConsumer);
            } else {
                resultSetEnd = this.internalReadResultSet(cumulateBuffer, serverStatusConsumer);
            }
            return resultSetEnd;
        }

        @Override
        public final void next(ResultRow row) {
            if (this.skipResultSetReader == null) {
                this.internalNext(row);
            }
        }

        @Override
        public final boolean isCancelled() {
            return this.task.hasError() || this.internalIsCancelled();
        }

        @Override
        public final void accept(ResultStatus status) {
            this.lastResultStatus = status;
            if (!this.lastResultSetEnd()) {
                this.internalAccept(status);
            }
        }

        @Override
        public final boolean hasMoreResult() {
            return Objects.requireNonNull(this.lastResultStatus, "this.lastResultStatus")
                    .hasMoreResult();
        }

        /**
         * @return true : task end.
         */
        abstract boolean internalNextUpdate(ResultStatus status);

        /**
         * @return true: ResultSet end
         */
        abstract boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);

        /**
         * @return true:  ResultSet end
         */
        final boolean skipResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            ResultSetReader resultSetReader = this.skipResultSetReader;
            if (resultSetReader == null) {
                resultSetReader = this.createResultSetReader();
                this.skipResultSetReader = resultSetReader;
            }
            return resultSetReader.read(cumulateBuffer, serverStatusConsumer);
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


        abstract boolean lastResultSetEnd();

        /**
         * <p>
         * if {@link #lastResultSetEnd()} is {@code true} don't invoke again.
         * </p>
         */
        abstract void internalAccept(ResultStatus status);

        void internalNext(ResultRow row) {
            assertNotSupportSubClass();
        }

        boolean internalIsCancelled() {
            assertNotSupportSubClass();
            return true;
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

    /**
     * <p>
     * This static inner class is base class of below classes:
     *     <ul>
     *         <li>{@link SingleModeBatchUpdateSink}</li>
     *         <li>{@link BindableSingleModeBatchUpdateSink}</li>
     *     </ul>
     * </p>
     */
    private static abstract class AbstractSingleModeBatchUpdateSink extends AbstractDownstreamSink {

        final FluxSink<ResultStatus> sink;

        private ResultStatus queryStatus;

        AbstractSingleModeBatchUpdateSink(ComQueryTask task, FluxSink<ResultStatus> sink) {
            super(task);
            this.sink = sink;
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.queryStatus != null;
        }

        @Override
        final boolean internalNextUpdate(ResultStatus status) {
            final boolean taskEnd;
            if (this.task.hasError()) {
                taskEnd = !status.hasMoreResult();
            } else if (status.hasMoreResult()) {
                taskEnd = false;
                if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsBatchUpdateMultiError);
                } else {
                    this.task.addError(TaskUtils.createBatchUpdateMultiError());
                }
            } else {
                this.sink.next(status); // drain to downstream
                if (this instanceof SingleModeBatchDownstreamSink) {
                    final SingleModeBatchDownstreamSink sink = (SingleModeBatchDownstreamSink) this;
                    taskEnd = !sink.hasMoreGroup() || sink.sendCommand();
                } else {
                    taskEnd = true;
                }

            }
            return taskEnd;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean resultSetEnd;
            resultSetEnd = this.skipResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                final ResultStatus status = Objects.requireNonNull(this.queryStatus, "this.queryStatus");
                if (status.hasMoreResult()) {
                    if (this.task.containException(SubscribeException.class)) {
                        this.task.replaceIfNeed(TaskUtils::replaceAsBatchUpdateMultiError);
                    } else {
                        this.task.addError(TaskUtils.createBatchUpdateMultiError());
                    }
                } else {
                    if (!this.task.containException(SubscribeException.class)) {
                        this.task.addError(TaskUtils.createBatchUpdateQueryError());
                    }

                }
            }
            return resultSetEnd;
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
        final void internalAccept(final ResultStatus status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s resultSetStatus non-null.", this));
            }
        }

        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.complete();
        }

    }// AbstractBatchUpdateDownstreamSink


    /**
     * <p>
     * This static inner class is underlying api implementation downstream sink of below methods:
     *     <ul>
     *         <li>{@link StaticStatement#executeQuery(String)}</li>
     *         <li>{@link StaticStatement#executeQuery(String, Consumer)}</li>
     *         <li>{@link BindableStatement#executeQuery()}</li>
     *         <li>{@link BindableStatement#executeQuery(Consumer)}</li>
     *     </ul>
     * </p>
     *
     * @see #ComQueryTask(Stmt, FluxSink, MySQLTaskAdjutant)
     * @see #ComQueryTask(FluxSink, BindableStmt, MySQLTaskAdjutant)
     */
    private final static class QueryDownstreamSink extends AbstractDownstreamSink {

        private final FluxSink<ResultRow> sink;

        private final Consumer<ResultStatus> statusConsumer;

        private final ResultSetReader resultSetReader;

        private ResultStatus queryStatus;

        /**
         * @see #ComQueryTask(Stmt, FluxSink, MySQLTaskAdjutant)
         * @see #ComQueryTask(FluxSink, BindableStmt, MySQLTaskAdjutant)
         */
        private QueryDownstreamSink(final ComQueryTask task, FluxSink<ResultRow> sink, Stmt stmt) {
            super(task);
            task.assertSingleMode(this);
            this.sink = sink;
            this.statusConsumer = stmt.getStatusConsumer();
            this.resultSetReader = this.createResultSetReader();
        }

        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            final boolean hasMoreResult = status.hasMoreResult();
            if (hasMoreResult) {
                if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsQueryMultiError);
                } else {
                    this.task.addError(TaskUtils.createQueryMultiError());
                }
            } else if (!this.task.containException(SubscribeException.class)) {
                this.task.addError(TaskUtils.createQueryUpdateError());
            }
            return !hasMoreResult;  // here ,maybe ,sql is that call stored procedure,skip rest results.
        }


        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean resultSetEnd;
            resultSetEnd = this.resultSetReader.read(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                final ResultStatus states = Objects.requireNonNull(this.queryStatus, "this.queryStatus");
                if (states.hasMoreResult()) {
                    // here ,sql is that call stored procedure,skip rest results.
                    if (this.task.containException(SubscribeException.class)) {
                        this.task.replaceIfNeed(TaskUtils::replaceAsQueryMultiError);
                    } else {
                        this.task.addError(TaskUtils.createQueryMultiError());
                    }

                }
            }
            return resultSetEnd;
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.queryStatus != null;
        }

        @Override
        final void internalAccept(final ResultStatus status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s resultSetStatus non-null.", this));
            }
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


        @Override
        public final void error(final JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            try {
                // invoke user ResultStates Consumer.
                this.statusConsumer.accept(Objects.requireNonNull(this.queryStatus, "this.status"));
                this.sink.complete();
            } catch (Throwable e) {
                this.sink.error(ResultStatusConsumerException.create(this.statusConsumer, e));
            }
        }


    }// QueryDownstreamSink

    /**
     * <p>
     * This static inner class is underlying api implementation downstream sink of below methods:
     *     <ul>
     *         <li>{@link StaticStatement#executeUpdate(String)}</li>
     *         <li>{@link BindableStatement#executeUpdate()}</li>
     *     </ul>
     * </p>
     *
     * @see #ComQueryTask(Stmt, MonoSink, MySQLTaskAdjutant)
     * @see #ComQueryTask(MonoSink, BindableStmt, MySQLTaskAdjutant)
     */
    private static final class UpdateDownstreamSink extends AbstractDownstreamSink {

        private final MonoSink<ResultStatus> sink;

        // update result status
        private ResultStatus updateStaus;

        // query result status
        private ResultStatus queryStatus;

        /**
         * @see #ComQueryTask(Stmt, MonoSink, MySQLTaskAdjutant)
         * @see #ComQueryTask(MonoSink, BindableStmt, MySQLTaskAdjutant)
         */
        private UpdateDownstreamSink(final ComQueryTask task, MonoSink<ResultStatus> sink) {
            super(task);
            task.assertSingleMode(this);
            this.sink = sink;
        }

        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            if (this.updateStaus == null && !this.task.hasError()) {
                this.updateStaus = status;
            }
            final boolean taskEnd;
            if (status.hasMoreResult()) {
                if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsUpdateMultiError);
                } else {
                    this.task.addError(TaskUtils.createUpdateMultiError());
                }
                taskEnd = false;
            } else {
                taskEnd = true;
            }
            return taskEnd;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean resultSetEnd;
            resultSetEnd = this.skipResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                final ResultStatus status = Objects.requireNonNull(this.queryStatus, "this.queryStatus");
                final boolean hasMorResult = status.hasMoreResult();
                if (hasMorResult) {
                    if (this.task.containException(SubscribeException.class)) {
                        this.task.replaceIfNeed(TaskUtils::replaceAsUpdateMultiError);
                    } else {
                        this.task.addError(TaskUtils.createUpdateMultiError());
                    }
                } else if (!this.task.containException(SubscribeException.class)) {
                    this.task.addError(TaskUtils.createUpdateQueryError());
                }
            }
            return resultSetEnd;
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.queryStatus != null;
        }

        @Override
        final void internalAccept(ResultStatus status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s queryStatus non-null.", this));
            }
        }

        @Override
        public final void error(final JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.success(Objects.requireNonNull(this.updateStaus, "this.updateStaus"));
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
    private static final class SingleModeBatchUpdateSink extends AbstractSingleModeBatchUpdateSink
            implements SingleModeBatchDownstreamSink {

        private final List<Stmt> stmtList;

        //start from 1 .
        private int index = 1;

        /**
         * @see #ComQueryTask(List, FluxSink, MySQLTaskAdjutant)
         * @see #ComQueryTask(FluxSink, BindableStmt, MySQLTaskAdjutant)
         */
        private SingleModeBatchUpdateSink(final ComQueryTask task, List<Stmt> stmtList
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
        public final boolean hasMoreGroup() {
            return this.index < this.stmtList.size();
        }

        @Override
        public boolean sendCommand() {
            final boolean taskEnd;
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
            return taskEnd;
        }

    }// SingleModeBatchUpdateSink

    /**
     * <p>
     * This static inner class is one of underlying implementation downstream sink of below methods.
     * <ul>
     *     <li>{@link BindableStatement#executeBatch()}</li>
     * </ul>
     * only when stmtList size less than 4 ,use this sink , or use {@link MultiModeBatchUpdateSink}
     * </p>
     *
     * @see MultiModeBatchUpdateSink
     */
    private static final class BindableSingleModeBatchUpdateSink extends AbstractSingleModeBatchUpdateSink
            implements SingleModeBatchDownstreamSink {

        private final String sql;

        private final List<List<BindValue>> groupList;

        // start from 1
        private int index = 1;

        /**
         * @see #ComQueryTask(FluxSink, BatchBindStmt, MySQLTaskAdjutant)
         */
        private BindableSingleModeBatchUpdateSink(final ComQueryTask task, final BatchBindStmt wrapper
                , FluxSink<ResultStatus> sink) {
            super(task, sink);
            this.sql = wrapper.getSql();
            this.groupList = wrapper.getGroupList();
            if (this.groupList.size() > 3) {
                throw new IllegalArgumentException("groupList size error.");
            }

        }

        @Override
        public final boolean hasMoreGroup() {
            return this.index < this.groupList.size();
        }

        @Override
        public final boolean sendCommand() {
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

    }//BindableSingleModeBatchUpdateSink


    /**
     * <p>
     * This static inner class is one of underlying implementation downstream sink of below methods.
     * <ul>
     *     <li>{@link StaticStatement#executeBatch(List)}</li>
     *     <li>{@link BindableStatement#executeBatch()}</li>
     * </ul>
     * only when stmtList size great than 3 ,use this sink , or use below:
     * <ul>
     *     <li>{@link SingleModeBatchUpdateSink}</li>
     *     <li>{@link BindableSingleModeBatchUpdateSink}</li>
     * </ul>
     * </p>
     *
     * @see SingleModeBatchUpdateSink
     * @see BindableSingleModeBatchUpdateSink
     */
    private static final class MultiModeBatchUpdateSink extends AbstractDownstreamSink {

        private final FluxSink<ResultStatus> sink;

        // query result status
        private ResultStatus queryStatus;

        // start from 0
        private int resultSequenceId = 0;

        /**
         * @see #ComQueryTask(List, FluxSink, MySQLTaskAdjutant)
         * @see #ComQueryTask(FluxSink, BatchBindStmt, MySQLTaskAdjutant)
         */
        private MultiModeBatchUpdateSink(final ComQueryTask task, FluxSink<ResultStatus> sink) {
            super(task);
            if (task.mode == Mode.SINGLE_STMT) {
                throw new IllegalStateException(String.format("mode[%s] error.", task.mode));
            }
            this.sink = sink;
        }

        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            final int currentSequenceId = this.resultSequenceId++;
            if (!this.task.hasError()) {
                if (currentSequenceId < this.task.sqlCount) {
                    this.sink.next(status);// drain to downstream
                } else {
                    this.task.addError(TaskUtils.createBatchUpdateMultiError());
                }
            }
            return !status.hasMoreResult();
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean resultSetEnd;
            resultSetEnd = this.skipResultSet(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                this.queryStatus = null; // clear for next query
                final int currentSequenceId = this.resultSequenceId++;
                if (currentSequenceId < this.task.sqlCount) {
                    if (!this.task.containException(SubscribeException.class)) {
                        this.task.addError(TaskUtils.createBatchUpdateQueryError());
                    }
                } else if (this.task.containException(SubscribeException.class)) {
                    this.task.replaceIfNeed(TaskUtils::replaceAsBatchUpdateMultiError);
                } else {
                    this.task.addError(TaskUtils.createBatchUpdateMultiError());
                }
            }
            return resultSetEnd;
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.queryStatus != null;
        }

        @Override
        final void internalAccept(final ResultStatus status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s resultStatus non-null.", this));
            }
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


    /**
     * <p>
     * This class is base class of below classes:
     *     <ul>
     *         <li>{@link MultiResultDownstreamSink}</li>
     *         <li>{@link SingleModeBatchBindMultiResultSink}</li>
     *         <li>{@link SingleModeBatchMultiResultSink}</li>
     *     </ul>
     * </p>
     */
    private static abstract class AbstractMultiResultDownstreamSink extends AbstractDownstreamSink {

        final MultiResultSink sink;

        final ResultSetReader resultSetReader;

        // query result status
        ResultStatus queryStatus;

        QuerySink querySink;

        AbstractMultiResultDownstreamSink(ComQueryTask task, MultiResultSink sink) {
            super(task);
            this.sink = sink;
            this.resultSetReader = this.createResultSetReader();
        }

        @Override
        final boolean internalNextUpdate(final ResultStatus status) {
            final boolean taskEnd;
            if (this.task.hasError()) {
                taskEnd = !status.hasMoreResult();
            } else {
                this.sink.nextUpdate(status);// drain to downstream

                if (status.hasMoreResult()) {
                    taskEnd = false;
                } else if (this instanceof SingleModeBatchDownstreamSink) {
                    final SingleModeBatchDownstreamSink sink = (SingleModeBatchDownstreamSink) this;
                    taskEnd = !sink.hasMoreGroup() || sink.sendCommand();
                } else {
                    taskEnd = true;
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

            final boolean resultSetEnd;
            resultSetEnd = this.resultSetReader.read(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                final ResultStatus status = Objects.requireNonNull(this.queryStatus, "this.status");

                this.querySink = null; // clear for next query
                this.queryStatus = null;// clear for next query

                if (!this.task.hasError()) {
                    querySink.accept(status); // drain to downstream
                    querySink.complete();// drain to downstream

                }
            }
            return resultSetEnd;
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
            return this.sink.isCancelled() || querySink.isCancelled();
        }

        @Override
        final void internalAccept(ResultStatus states) {
            if (this.queryStatus != null) {
                // here,bug
                throw new IllegalStateException(String.format("%s.status not null.", this));
            }
            this.queryStatus = states;
        }


    }

    /**
     * <p>
     * This class is one of underlying api implementation downstream sink of below methods:
     *     <ul>
     *         <li>{@link StaticStatement#executeAsMulti(List)}</li>
     *         <li>{@link StaticStatement#executeAsFlux(List)}</li>
     *         <li>{@link BindableStatement#executeAsMulti()}</li>
     *         <li>{@link BindableStatement#executeAsFlux()}</li>
     *         <li>{@link MultiStatement#executeAsMulti()}</li>
     *         <li>{@link MultiStatement#executeAsFlux()}</li>
     *     </ul>
     * </p>
     * <p>
     *     When {@link #mode} isn't {@link Mode#SINGLE_STMT}  use this class ,or use below:
     *     <ul>
     *         <li>{@link SingleModeBatchBindMultiResultSink}</li>
     *         <li>{@link SingleModeBatchMultiResultSink}</li>
     *     </ul>
     * </p>
     */
    private static final class MultiResultDownstreamSink extends AbstractMultiResultDownstreamSink {

        /**
         * @see #ComQueryTask(List, MultiResultSink, MySQLTaskAdjutant)
         * @see #ComQueryTask(MultiResultSink, BatchBindStmt, MySQLTaskAdjutant)
         */
        private MultiResultDownstreamSink(final ComQueryTask task, MultiResultSink sink) {
            super(task, sink);
        }


    }// MultiResultDownstreamSink

    /**
     * <p>
     * This class is one of underlying api implementation downstream sink of below methods:
     *     <ul>
     *         <li>{@link StaticStatement#executeAsMulti(List)}</li>
     *         <li>{@link StaticStatement#executeAsFlux(List)}</li>
     *     </ul>
     * </p>
     * <p>
     *     When {@link #mode} is {@link Mode#SINGLE_STMT}  use this class ,or use below:
     *     <ul>
     *         <li>{@link MultiResultDownstreamSink}</li>
     *     </ul>
     * </p>
     */
    private static final class SingleModeBatchMultiResultSink extends AbstractMultiResultDownstreamSink
            implements SingleModeBatchDownstreamSink {

        private final List<Stmt> stmtList;

        //start from 1 .
        private int index = 1;

        private SingleModeBatchMultiResultSink(final ComQueryTask task, List<Stmt> stmtList
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
        public final boolean hasMoreGroup() {
            return this.index < this.stmtList.size();
        }

        @Override
        public final boolean sendCommand() {
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
     * This class is one of underlying api implementation downstream sink of below methods:
     *     <ul>
     *         <li>{@link BindableStatement#executeAsMulti()}</li>
     *         <li>{@link BindableStatement#executeAsFlux()}</li>
     *     </ul>
     * </p>
     * <p>
     *     When {@link #mode} is {@link Mode#SINGLE_STMT}  use this class ,or use below:
     *     <ul>
     *         <li>{@link MultiResultDownstreamSink}</li>
     *     </ul>
     * </p>
     */
    private static final class SingleModeBatchBindMultiResultSink extends AbstractMultiResultDownstreamSink
            implements SingleModeBatchDownstreamSink {

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
        public final boolean hasMoreGroup() {
            return this.index < this.groupList.size();
        }

        @Override
        public final boolean sendCommand() {
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


    }// SingleModeBatchBindMultiResultSink


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
