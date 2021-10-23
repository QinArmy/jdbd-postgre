package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.stmt.MySQLStmt;
import io.jdbd.mysql.stmt.QueryAttr;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.type.CodeEnum;
import io.jdbd.vendor.stmt.*;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.*;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.*;


/**
 * @see ComPreparedTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
 */
final class PrepareExecuteCommandWriter implements ExecuteCommandWriter {

    static PrepareExecuteCommandWriter create(final PrepareStmtTask stmtTask) {
        return new PrepareExecuteCommandWriter(stmtTask);
    }


    private final PrepareStmtTask stmtTask;

    private final ParamSingleStmt stmt;

    private final TaskAdjutant adjutant;

    private final int capability;

    private final boolean supportQueryAttr;

    private final MySQLServerVersion serverVersion;


    private PrepareExecuteCommandWriter(final PrepareStmtTask stmtTask) {
        this.stmtTask = stmtTask;
        this.stmt = stmtTask.getStmt();
        this.adjutant = stmtTask.adjutant();
        this.capability = this.adjutant.capability();

        this.supportQueryAttr = Capabilities.supportQueryAttr(this.capability);
        this.serverVersion = this.adjutant.handshake10().getServerVersion();
        ;
    }


    @Override
    public Publisher<ByteBuf> writeCommand(final int batchIndex) throws SQLException {
        final List<? extends ParamValue> bindGroup;
        bindGroup = getBindGroup(batchIndex);

        final MySQLColumnMeta[] paramMetaArray = Objects.requireNonNull(this.stmtTask.getParameterMetas());
        MySQLBinds.assertParamCountMatch(batchIndex, paramMetaArray.length, bindGroup.size());

        int longDataCount = 0;
        for (int i = 0; i < paramMetaArray.length; i++) {
            final ParamValue paramValue = bindGroup.get(i);
            if (paramValue.getIndex() != i) {
                // hear invoker has bug
                throw MySQLExceptions.createBindValueParamIndexNotMatchError(batchIndex, paramValue, i);
            }
            final Object value = paramValue.get();
            if (value instanceof Publisher || value instanceof Path) {
                longDataCount++;
            }
        }
        final Publisher<ByteBuf> publisher;
        if (paramMetaArray.length == 0 && !this.supportQueryAttr) {
            // this 'if' block handle no bind parameter.
            final ByteBuf packet;
            packet = createExecutePacket(10);
            publisher = Packets.createPacketPublisher(packet, this.stmtTask::addAndGetSequenceId, this.adjutant);
        } else if (longDataCount == 0) {
            // this 'if' block handle no long parameter.
            publisher = bindParameters(batchIndex, bindGroup);
        } else {
            // start safe sequence id
            publisher = new PrepareLongParameterWriter(this.stmtTask)
                    .write(batchIndex, bindGroup)
                    .concatWith(defferBindParameters(batchIndex, bindGroup));
        }
        return publisher;
    }

    /*################################## blow private method ##################################*/

    private List<? extends ParamValue> getBindGroup(final int batchIndex) {
        ParamSingleStmt stmt = this.stmt;
        if (stmt instanceof PrepareStmt) {
            stmt = ((PrepareStmt) stmt).getStmt();
        }
        final List<? extends ParamValue> bindGroup;
        if (stmt instanceof ParamStmt) {
            if (batchIndex > -1) {
                String m = String.format("batchIndex[%s] isn't negative for stmt[%s].", batchIndex, stmt);
                throw new IllegalArgumentException(m);
            }
            bindGroup = ((ParamStmt) stmt).getBindGroup();
        } else if (stmt instanceof ParamBatchStmt) {
            final ParamBatchStmt<? extends ParamValue> batchStmt = (ParamBatchStmt<? extends ParamValue>) stmt;
            if (batchIndex >= batchStmt.getGroupList().size()) {
                String m = String.format("batchIndex[%s] great or equal than group size[%s] for stmt[%s]."
                        , batchIndex, batchStmt.getGroupList().size(), stmt);
                throw new IllegalArgumentException(m);
            }
            bindGroup = batchStmt.getGroupList().get(batchIndex);
        } else {
            // here bug
            String m = String.format("Unknown stmt type %s", stmt);
            throw new IllegalStateException(m);
        }
        return bindGroup;
    }

    private Map<String, QueryAttr> getQueryAttribute() {
        ParamSingleStmt stmt = this.stmt;
        if (stmt instanceof PrepareStmt) {
            stmt = ((PrepareStmt) stmt).getStmt();
        }
        final Map<String, QueryAttr> queryAttrMap;
        if (stmt instanceof MySQLStmt) {
            queryAttrMap = ((MySQLStmt) stmt).getQueryAttrs();
        } else {
            queryAttrMap = Collections.emptyMap();
        }
        return queryAttrMap;
    }

    private Publisher<ByteBuf> defferBindParameters(final int batchIndex, final List<? extends ParamValue> bindGroup) {
        return Flux.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                defferBIndParamInEventLoop(batchIndex, bindGroup, sink);
            } else {
                this.adjutant.execute(() -> defferBIndParamInEventLoop(batchIndex, bindGroup, sink));
            }
        });
    }

    private void defferBIndParamInEventLoop(final int batchIndex, final List<? extends ParamValue> bindGroup
            , final FluxSink<ByteBuf> sink) {
        try {
            Flux.from(bindParameters(batchIndex, bindGroup))
                    .subscribe(sink::next, sink::error, sink::complete);
        } catch (Throwable e) {
            this.stmtTask.addErrorToTask(e);
            this.stmtTask.handleNoExecuteMessage();
            sink.complete();
        }
    }


    /**
     * @return {@link Flux} that is created by {@link Flux#fromIterable(Iterable)} method.
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
     */
    private Publisher<ByteBuf> bindParameters(final int batchIndex, final List<? extends ParamValue> bindGroup)
            throws JdbdException, SQLException {


        final MySQLColumnMeta[] paramMetaArray = this.stmtTask.getParameterMetas();
        MySQLBinds.assertParamCountMatch(batchIndex, paramMetaArray.length, bindGroup.size());

        final Map<String, QueryAttr> queryAttrMap = getQueryAttribute();

        final ByteBuf packet;
        packet = createExecutePacket(1024);

        try {
            final boolean supportQueryAttr = this.supportQueryAttr;
            final int paramCount;
            if (supportQueryAttr) {
                paramCount = paramMetaArray.length + queryAttrMap.size();
                Packets.writeIntLenEnc(packet, paramCount); // parameter_count
            } else {
                paramCount = paramMetaArray.length;
            }
            final boolean zoneSuffix = this.serverVersion.meetsMinimum(MySQLServerVersion.V8_0_19);

            final byte[] nullBitsMap = new byte[(paramCount + 7) >> 3];
            final int nullBitsMapIndex = packet.writerIndex();
            packet.writeZero(nullBitsMap.length); // placeholder for fill null_bitmap
            packet.writeByte(1); //fill new_params_bind_flag

            //1. make nullBitsMap and fill  parameter_types
            final List<MySQLType> bindTypeList = new ArrayList<>(paramCount);
            for (int i = 0; i < paramMetaArray.length; i++) {
                final ParamValue paramValue = bindGroup.get(i);
                final Object value = paramValue.get();
                if (value instanceof Publisher || value instanceof Path) {
                    // long parameter
                    bindTypeList.add(MySQLType.NULL);// filler
                    continue;
                }
                final MySQLType bindType;
                if (value == null) {
                    nullBitsMap[i >> 3] |= (1 << (i & 7));
                    bindType = paramMetaArray[i].sqlType; // filler
                } else {
                    bindType = paramMetaArray[i].sqlType;
                    // bindType = decideBindType(batchIndex, paramMetaArray[i], paramValue);
                }
                bindTypeList.add(bindType);
                Packets.writeInt2(packet, bindType.parameterType);
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
                    final MySQLType bindType = queryAttr.getType();
                    bindTypeList.add(bindType);
                    Packets.writeInt2(packet, bindType.parameterType);
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
                MySQLType type = bindTypeList.get(i);
                switch (type) {
                    case TIME:
                    case DATETIME:
                    case TIMESTAMP:
                        precision = paramMetaArray[i].getDateTimeTypePrecision();
                        break;
                    default:
                        precision = 0;
                }
                BinaryWriter.writeNonNullBinary(packet, batchIndex, type, paramValue, precision, clientCharset);

            }
            // below write query attribute
            for (QueryAttr queryAttr : attrList) {
                // use precision 6 ,because query attribute no metadata.
                BinaryWriter.writeNonNullBinary(packet, batchIndex, queryAttr.getType(), queryAttr, 6, clientCharset);
            }

            return Packets.createPacketPublisher(packet, this.stmtTask::addAndGetSequenceId, this.adjutant);

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
        int flags = Constants.CURSOR_TYPE_NO_CURSOR;
        if (this.stmtTask.supportFetch()) {
            flags |= Constants.CURSOR_TYPE_READ_ONLY;
        }
        if (this.supportQueryAttr) {
            flags |= Constants.PARAMETER_COUNT_AVAILABLE;
        }
        packet.writeByte(flags); // flags
        Packets.writeInt4(packet, 1);//4. iteration_count,Number of times to execute the statement. Currently, always 1.

        return packet;
    }

    private MySQLType decideBindType(final int stmtIndex, MySQLColumnMeta meta, ParamValue paramValue) {
        final Object nonNull = paramValue.getNonNull();
        final MySQLType targetType = meta.sqlType;
        final MySQLType bindType;
        if (nonNull instanceof Number) {
            if (nonNull instanceof Long) {
                bindType = MySQLType.BIGINT;
            } else if (nonNull instanceof Integer) {
                bindType = MySQLType.INT;
            } else if (nonNull instanceof Short) {
                bindType = MySQLType.SMALLINT;
            } else if (nonNull instanceof Byte) {
                bindType = MySQLType.TINYINT;
            } else if (nonNull instanceof Double) {
                bindType = MySQLType.DOUBLE;
            } else if (nonNull instanceof Float) {
                bindType = MySQLType.FLOAT;
            } else if (nonNull instanceof BigDecimal || nonNull instanceof BigInteger) {
                bindType = MySQLType.DECIMAL;
            } else {
                throw MySQLExceptions.createWrongArgumentsException(stmtIndex, meta.sqlType, paramValue, null);
            }
        } else if (nonNull instanceof Boolean) {
            if (targetType == MySQLType.CHAR
                    || targetType == MySQLType.VARCHAR) {
                bindType = targetType;
            } else {
                bindType = MySQLType.TINYINT;
            }
        } else if (nonNull instanceof String) {
            if (targetType == MySQLType.DATETIME
                    || targetType == MySQLType.TIMESTAMP
                    || targetType == MySQLType.TIME
                    || targetType == MySQLType.JSON) {
                bindType = targetType;
            } else {
                bindType = MySQLType.VARCHAR;
            }
        } else if (nonNull instanceof Temporal) {
            if (nonNull instanceof LocalDateTime
                    || nonNull instanceof OffsetDateTime
                    || nonNull instanceof ZonedDateTime) {
                bindType = MySQLType.DATETIME;
            } else if (nonNull instanceof LocalDate
                    || nonNull instanceof YearMonth) {
                bindType = MySQLType.DATE;
            } else if (nonNull instanceof LocalTime
                    || nonNull instanceof OffsetTime) {
                bindType = MySQLType.TIME;
            } else if (nonNull instanceof Year) {
                bindType = MySQLType.SMALLINT;
            } else if (nonNull instanceof Instant) {
                bindType = MySQLType.BIGINT;
            } else {
                throw MySQLExceptions.createWrongArgumentsException(stmtIndex, meta.sqlType, paramValue, null);
            }
        } else if (nonNull instanceof byte[]) {
            bindType = MySQLType.VARBINARY;
        } else if (nonNull instanceof TemporalAccessor) {
            if (targetType == MySQLType.JSON) {
                bindType = targetType;
            } else if (nonNull instanceof MonthDay) {
                bindType = MySQLType.DATE;
            } else if (nonNull instanceof Month
                    || nonNull instanceof DayOfWeek) {
                if (targetType == MySQLType.ENUM
                        || targetType == MySQLType.CHAR
                        || targetType == MySQLType.VARCHAR) {
                    bindType = targetType;
                } else {
                    bindType = MySQLType.TINYINT;
                }
            } else if (nonNull instanceof ZoneOffset) {
                bindType = MySQLType.INT;
            } else {
                throw MySQLExceptions.createWrongArgumentsException(stmtIndex, meta.sqlType, paramValue, null);
            }
        } else if (nonNull instanceof TemporalAmount) {
            if (nonNull instanceof Duration) {
                bindType = MySQLType.TIME;
            } else {
                throw MySQLExceptions.createWrongArgumentsException(stmtIndex, meta.sqlType, paramValue, null);
            }
        } else if (nonNull instanceof Enum) {
            if (targetType == MySQLType.ENUM
                    || targetType == MySQLType.CHAR
                    || targetType == MySQLType.VARCHAR) {
                bindType = targetType;
            } else {
                bindType = MySQLType.CHAR;
            }
        } else if (nonNull instanceof CodeEnum
                || nonNull instanceof ZoneId) {
            bindType = MySQLType.INT;
        } else if (nonNull instanceof Set) {
            bindType = MySQLType.VARCHAR;
        } else {
            throw MySQLExceptions.createWrongArgumentsException(stmtIndex, meta.sqlType, paramValue, null);
        }
        return bindType;
    }



}
