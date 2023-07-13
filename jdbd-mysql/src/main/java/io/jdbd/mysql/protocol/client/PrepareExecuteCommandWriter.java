package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.stmt.*;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;


/**
 * @see ComPreparedTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
 */
final class PrepareExecuteCommandWriter implements ExecuteCommandWriter {


    static PrepareExecuteCommandWriter create(final PrepareStmtTask stmtTask) {
        return new PrepareExecuteCommandWriter(stmtTask);
    }

    private final Logger LOG = LoggerFactory.getLogger(PrepareExecuteCommandWriter.class);


    // below enum_cursor_type @see https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a3e5e9e744ff6f7b989a604fd669977da
    private static final byte CURSOR_TYPE_NO_CURSOR = 0;
    private static final byte CURSOR_TYPE_READ_ONLY = 1;
    private static final byte PARAMETER_COUNT_AVAILABLE = 1 << 3;

    private final PrepareStmtTask stmtTask;

    private final ParamSingleStmt stmt;

    private final TaskAdjutant adjutant;

    private final int capability;

    private final boolean supportQueryAttr;

    private final MySQLServerVersion serverVersion;

    private LongParameterWriter longParamWriter;


    private PrepareExecuteCommandWriter(final PrepareStmtTask stmtTask) {
        this.stmtTask = stmtTask;
        this.stmt = stmtTask.getStmt();
        this.adjutant = stmtTask.adjutant();
        this.capability = this.adjutant.capability();

        this.supportQueryAttr = Capabilities.supportQueryAttr(this.capability);
        this.serverVersion = this.adjutant.handshake10().serverVersion;
    }


    @Override
    public Publisher<ByteBuf> writeCommand(final int batchIndex) throws SQLException {
        final List<ParamValue> bindGroup;
        bindGroup = getBindGroup(batchIndex);

        final MySQLColumnMeta[] paramMetaArray = Objects.requireNonNull(this.stmtTask.getParameterMetas());
        MySQLBinds.assertParamCountMatch(batchIndex, paramMetaArray.length, bindGroup.size());

        List<ParamValue> longParamList = null;
        ParamValue paramValue;
        Object value;
        for (int i = 0; i < paramMetaArray.length; i++) {
            paramValue = bindGroup.get(i);
            if (paramValue.getIndex() != i) {
                // hear invoker has bug
                throw MySQLExceptions.bindValueParamIndexNotMatchError(batchIndex, paramValue, i);
            }
            value = paramValue.getValue();
            if (value instanceof Publisher || value instanceof Path) {
                if (longParamList == null) {
                    longParamList = MySQLCollections.arrayList();
                }
                longParamList.add(paramValue);
            }
        }
        final Publisher<ByteBuf> publisher;
        if (paramMetaArray.length == 0 && (!this.supportQueryAttr || this.stmt.getStmtVarList().size() == 0)) {
            // this 'if' block handle no bind parameter.
            final ByteBuf packet;
            packet = createExecutePacket(10);
            this.stmtTask.resetSequenceId(); // reset sequenceId before write header
            publisher = Packets.createPacketPublisher(packet, this.stmtTask::nextSequenceId, this.adjutant);
        } else if (longParamList == null || longParamList.size() == 0) {
            // this 'if' block handle no long parameter.
            publisher = bindParameters(batchIndex, bindGroup);
        } else {
            // start next group need reset
            LongParameterWriter longParamWriter = this.longParamWriter;
            if (longParamWriter == null) {
                this.longParamWriter = longParamWriter = new PrepareLongParameterWriter(this.stmtTask);
            }
            publisher = longParamWriter.write(batchIndex, longParamList)
                    .concatWith(defferBindParameters(batchIndex, bindGroup))
                    .onErrorResume(this::handleSendError);
        }
        return publisher;
    }

    /*################################## blow private method ##################################*/

    private <T> Publisher<T> handleSendError(final Throwable e) {
        final Mono<T> empty;
        if (this.adjutant.inEventLoop()) {
            this.stmtTask.addErrorToTask(e);
            this.stmtTask.handleExecuteMessageError();
            empty = Mono.empty();
        } else {
            empty = Mono.create(sink -> this.adjutant.execute(() -> {
                this.stmtTask.addErrorToTask(e);
                this.stmtTask.handleExecuteMessageError();
                sink.success();
            }));
        }
        return empty;
    }


    private List<ParamValue> getBindGroup(final int batchIndex) {
        ParamSingleStmt stmt = this.stmt;
        if (stmt instanceof PrepareStmt) {
            stmt = ((PrepareStmt) stmt).getStmt();
        }
        final List<ParamValue> bindGroup;
        if (stmt instanceof ParamStmt) {
            if (batchIndex > -1) {
                String m = String.format("batchIndex[%s] isn't negative for stmt[%s].", batchIndex, stmt);
                throw new IllegalArgumentException(m);
            }
            bindGroup = ((ParamStmt) stmt).getBindGroup();
        } else if (stmt instanceof ParamBatchStmt) {
            final ParamBatchStmt batchStmt = (ParamBatchStmt) stmt;
            final List<List<ParamValue>> groupList = batchStmt.getGroupList();
            if (batchIndex >= groupList.size()) {
                String m = String.format("batchIndex[%s] great or equal than group size[%s] for stmt[%s]."
                        , batchIndex, batchStmt.getGroupList().size(), stmt);
                throw new IllegalArgumentException(m);
            }
            bindGroup = groupList.get(batchIndex);
        } else {
            // here bug
            String m = String.format("Unknown stmt type %s", stmt);
            throw new IllegalStateException(m);
        }
        return bindGroup;
    }


    private Publisher<ByteBuf> defferBindParameters(final int batchIndex, final List<ParamValue> bindGroup) {
        return Flux.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                defferBIndParamInEventLoop(batchIndex, bindGroup, sink);
            } else {
                this.adjutant.execute(() -> defferBIndParamInEventLoop(batchIndex, bindGroup, sink));
            }
        });
    }

    private void defferBIndParamInEventLoop(final int batchIndex, final List<ParamValue> bindGroup,
                                            final FluxSink<ByteBuf> sink) {
        try {
            Flux.from(bindParameters(batchIndex, bindGroup))
                    .subscribe(sink::next, sink::error, sink::complete);
        } catch (Throwable e) {
            this.stmtTask.addErrorToTask(e);
            this.stmtTask.handleExecuteMessageError();
            sink.complete();
        }
    }


    /**
     * @return {@link Flux} that is created by {@link Flux#fromIterable(Iterable)} method.
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
     */
    private Publisher<ByteBuf> bindParameters(final int batchIndex, final List<ParamValue> bindGroup)
            throws JdbdException {

        final MySQLColumnMeta[] paramMetaArray = this.stmtTask.getParameterMetas();
        MySQLBinds.assertParamCountMatch(batchIndex, paramMetaArray.length, bindGroup.size());

        final List<NamedValue> queryAttrList = this.stmt.getStmtVarList();

        final ByteBuf packet;
        packet = createExecutePacket(1024);

        try {
            final boolean supportQueryAttr = this.supportQueryAttr;
            final int paramCount;
            if (supportQueryAttr) {
                paramCount = paramMetaArray.length + queryAttrList.size();
                Packets.writeIntLenEnc(packet, paramCount); // parameter_count
            } else {
                paramCount = paramMetaArray.length;
            }

            final byte[] nullBitsMap = new byte[(paramCount + 7) >> 3];
            final int nullBitsMapIndex = packet.writerIndex();
            packet.writeZero(nullBitsMap.length); // placeholder for fill null_bitmap
            packet.writeByte(1); //fill new_params_bind_flag

            //1. make nullBitsMap and fill  parameter_types
            for (int i = 0; i < paramMetaArray.length; i++) {
                final ParamValue paramValue = bindGroup.get(i);
                final MySQLType actualType;
                if (paramValue.get() == null) {
                    nullBitsMap[i >> 3] |= (1 << (i & 7));
                    actualType = paramMetaArray[i].sqlType;
                } else if (paramValue.isLongData()) {
                    actualType = paramMetaArray[i].sqlType;
                } else {
                    actualType = BinaryWriter.decideActualType(paramMetaArray[i].sqlType, paramValue);
                }
                Packets.writeInt2(packet, actualType.parameterType);
                if (supportQueryAttr) {
                    packet.writeByte(0); //string<lenenc> parameter_name
                }
            }

            final Charset clientCharset = this.adjutant.charsetClient();
            final List<QueryAttr> attrList;
            if (supportQueryAttr) {
                attrList = new ArrayList<>(queryAttrMap.size());
                int queryAttrIndex = paramMetaArray.length;
                for (Map.Entry<String, QueryAttr> e : queryAttrMap.entrySet()) {
                    final QueryAttr queryAttr = e.getValue();
                    attrList.add(queryAttr); // store  Query Attribute
                    if (queryAttr.get() == null) {
                        nullBitsMap[queryAttrIndex >> 3] |= (1 << (queryAttrIndex & 7));
                    }
                    final MySQLType actualType = BinaryWriter.decideActualType(queryAttr.getType(), queryAttr);
                    Packets.writeInt2(packet, actualType.parameterType);
                    Packets.writeStringLenEnc(packet, e.getKey().getBytes(clientCharset));
                    queryAttrIndex++;
                }
            } else {
                attrList = Collections.emptyList();
            }


            final int writeIndex = packet.writerIndex();
            packet.writerIndex(nullBitsMapIndex);
            packet.writeBytes(nullBitsMap); //fill null_bitmap
            packet.writerIndex(writeIndex); // reset writeIndex

            // below write bind parameter values
            for (int i = 0, precision; i < paramMetaArray.length; i++) {
                ParamValue paramValue = bindGroup.get(i);
                Object value = paramValue.get();
                if (value == null || value instanceof Publisher || value instanceof Path) {
                    continue;
                }
                final MySQLColumnMeta meta = paramMetaArray[i];
                final MySQLType expected = meta.sqlType;
                switch (expected) {
                    case TIME:
                    case DATETIME:
                    case TIMESTAMP:
                        precision = meta.getDateTimeTypePrecision();
                        break;
                    default:
                        precision = 0;
                }
                BinaryWriter.writeBinary(packet, batchIndex, expected, paramValue, precision, clientCharset);

            }
            // below write query attribute
            for (QueryAttr queryAttr : attrList) {
                // use precision 6 ,because query attribute no metadata.
                BinaryWriter.writeBinary(packet, batchIndex, queryAttr.getType(), queryAttr, 6, clientCharset);
            }

            this.stmtTask.resetSequenceId(); // reset sequenceId before write header
            return Packets.createPacketPublisher(packet, this.stmtTask::nextSequenceId, this.adjutant);

        } catch (Throwable e) {
            packet.release();
            throw e;

        }
    }


    /**
     * @see #bindParameters(int, List)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
     */
    private ByteBuf createExecutePacket(final int capacity) {

        final ByteBuf packet;
        packet = this.adjutant.allocator().buffer(Packets.HEADER_SIZE + capacity, Integer.MAX_VALUE);
        packet.writeZero(Packets.HEADER_SIZE); // placeholder of header

        packet.writeByte(Packets.COM_STMT_EXECUTE); // 1.status
        Packets.writeInt4(packet, this.stmtTask.getStatementId());// 2. statement_id
        //3.cursor Flags, reactive api not support cursor
        int flags = CURSOR_TYPE_NO_CURSOR;
        if (this.stmtTask.isSupportFetch()) {
            flags |= CURSOR_TYPE_READ_ONLY;
        }
        if (this.supportQueryAttr) {
            flags |= PARAMETER_COUNT_AVAILABLE;
        }
        packet.writeByte(flags); // flags
        Packets.writeInt4(packet, 1);//4. iteration_count,Number of times to execute the statement. Currently, always 1.

        return packet;
    }


}
