package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLNumberUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.jdbd.vendor.statement.ParamValue;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


/**
 * @see ComPreparedTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
 */
final class PrepareExecuteCommandWriter implements StatementCommandWriter {

    private static final Logger LOG = LoggerFactory.getLogger(PrepareExecuteCommandWriter.class);

    private final StatementTask statementTask;

    private final int statementId;

    private final MySQLColumnMeta[] paramMetaArray;

    private final ClientProtocolAdjutant adjutant;

    private final boolean fetchResultSet;


    PrepareExecuteCommandWriter(final StatementTask statementTask) {
        this.statementTask = statementTask;
        this.statementId = statementTask.obtainStatementId();
        this.paramMetaArray = statementTask.obtainParameterMetas();
        this.adjutant = statementTask.obtainAdjutant();

        this.fetchResultSet = statementTask.isFetchResult();
    }


    @Override
    public Publisher<ByteBuf> writeCommand(final int stmtIndex, final List<? extends ParamValue> parameterGroup)
            throws SQLException {
        final MySQLColumnMeta[] paramMetaArray = this.paramMetaArray;
        BindUtils.assertParamCountMatch(stmtIndex, paramMetaArray.length, parameterGroup.size());

        int nonLongDataCount = 0;
        for (int i = 0; i < paramMetaArray.length; i++) {
            ParamValue paramValue = parameterGroup.get(i);
            if (paramValue.getParamIndex() != i) {
                // hear invoker has bug
                throw MySQLExceptions.createBindValueParamIndexNotMatchError(stmtIndex, paramValue, i);
            }
            if (!paramValue.isLongData()) {
                nonLongDataCount++;
            }
        }

        final Publisher<ByteBuf> publisher;
        if (paramMetaArray.length == 0) {
            // this 'if' block handle no bind parameter.
            ByteBuf packet = createExecutePacketBuffer(10);
            PacketUtils.writePacketHeader(packet, this.statementTask.addAndGetSequenceId());
            publisher = Mono.just(packet);
        } else {
            final Publisher<ByteBuf> nonStreamPublisher;
            nonStreamPublisher = createExecutionPacketPublisher(stmtIndex, parameterGroup);
            if (nonLongDataCount == paramMetaArray.length) {
                // this 'if' block handle no long parameter.
                publisher = nonStreamPublisher;
            } else {
                publisher = new PrepareLongParameterWriter(statementTask)
                        .write(stmtIndex, parameterGroup)
                        .concatWith(nonStreamPublisher);
            }
        }
        return publisher;
    }

    /*################################## blow private method ##################################*/


    /**
     * @see #writeCommand(int, List)
     */
    private Flux<ByteBuf> createExecutionPacketPublisher(int stmtIndex, List<? extends ParamValue> parameterGroup) {
        Flux<ByteBuf> flux;
        try {
            flux = doCreateExecutionPackets(stmtIndex, parameterGroup);
        } catch (Throwable e) {
            flux = Flux.error(MySQLExceptions.wrap(e));
        }
        return flux;
    }

    /**
     * @return {@link Flux} that is created by {@link Flux#fromIterable(Iterable)} method.
     * @see #createExecutionPacketPublisher(int, List)
     */
    private Flux<ByteBuf> doCreateExecutionPackets(final int stmtIndex, final List<? extends ParamValue> parameterGroup)
            throws JdbdException {
        final MySQLColumnMeta[] parameterMetaArray = this.paramMetaArray;
        final byte[] nullBitsMap = new byte[(parameterMetaArray.length + 7) >> 3];

        ByteBuf packet;
        packet = createExecutePacketBuffer(1024);
        final int nullBitsMapIndex = packet.writerIndex();
        packet.writeZero(nullBitsMap.length); // placeholder for fill null_bitmap
        packet.writeByte(1); //fill new_params_bind_flag

        //1. make nullBitsMap and parameterValueLength
        for (int i = 0; i < parameterMetaArray.length; i++) {
            ParamValue paramValue = parameterGroup.get(i);
            if (paramValue.getValue() == null) {
                nullBitsMap[i >> 3] |= (1 << (i & 7));
            }
            MySQLType mySQLType = decideBindByte(stmtIndex, parameterMetaArray[i], paramValue);
            //fill  parameter_types
            PacketUtils.writeInt2(packet, mySQLType.parameterType);
        }

        final int writeIndex = packet.writerIndex();
        packet.writerIndex(nullBitsMapIndex);

        packet.writeBytes(nullBitsMap); //fill null_bitmap

        packet.writerIndex(writeIndex); // reset writeIndex

        //fill parameter_values
        LinkedList<ByteBuf> packetList = new LinkedList<>();
        Flux<ByteBuf> flux;
        try {
            ParamValue paramValue;
            final int maxAllowedPayload = this.adjutant.obtainHostInfo().maxAllowedPayload();
            int wroteBytes = 0;

            for (int i = 0; i < parameterMetaArray.length; i++) {
                paramValue = parameterGroup.get(i);
                if (paramValue.isLongData() || paramValue.getValue() == null) {
                    continue;
                }
                while (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    ByteBuf temp = packet.readRetainedSlice(PacketUtils.MAX_PACKET);
                    PacketUtils.writePacketHeader(temp, this.statementTask.addAndGetSequenceId());
                    packetList.add(temp);
                    wroteBytes += PacketUtils.MAX_PAYLOAD;

                    temp = this.adjutant.createPacketBuffer(Math.min(1024, packet.readableBytes()));
                    temp.writeBytes(packet);
                    packet.release();
                    packet = temp;
                }
                if (wroteBytes < 0 || wroteBytes > maxAllowedPayload) {
                    throw MySQLExceptions.createNetPacketTooLargeException(maxAllowedPayload);
                }
                // bind parameter bto packet buffer
                bindParameter(packet, stmtIndex, parameterMetaArray[i], paramValue);
            }
            wroteBytes += (packet.readableBytes() - PacketUtils.HEADER_SIZE);
            if (wroteBytes < 0 || wroteBytes > maxAllowedPayload) {
                throw MySQLExceptions.createNetPacketTooLargeException(maxAllowedPayload);
            }

            PacketUtils.writePacketHeader(packet, this.statementTask.addAndGetSequenceId());
            packetList.add(packet);


            flux = Flux.fromIterable(packetList);
        } catch (Throwable e) {
            BindUtils.releaseOnError(packetList, packet);
            flux = Flux.error(MySQLExceptions.wrap(e));
        }
        return flux;
    }

    private MySQLType decideBitBindType(int stmtIndex, ParamValue paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final MySQLType mySQLType;
        if (nonNull instanceof byte[] && (((byte[]) nonNull).length & 7) == 0) {
            mySQLType = MySQLType.BIT;
        } else if (nonNull instanceof Long
                || nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            mySQLType = MySQLType.BIGINT;
        } else if (nonNull instanceof String) {
            mySQLType = MySQLType.CHAR;
        } else if (nonNull instanceof BigDecimal
                || nonNull instanceof BigInteger) {
            mySQLType = MySQLType.DECIMAL;
        } else {
            throw MySQLExceptions.createWrongArgumentsException(stmtIndex, MySQLType.BIT, paramValue, null);
        }
        return mySQLType;
    }

    /**
     * @see #doCreateExecutionPackets(int, List)
     */
    private ByteBuf createExecutePacketBuffer(int initialPayloadCapacity) {

        ByteBuf packet = this.adjutant.createPacketBuffer(Math.min(initialPayloadCapacity, PacketUtils.MAX_PAYLOAD));

        packet.writeByte(PacketUtils.COM_STMT_EXECUTE); // 1.status
        PacketUtils.writeInt4(packet, this.statementId);// 2. statement_id
        //3.cursor Flags, reactive api not support cursor
        if (this.fetchResultSet) {
            packet.writeByte(ProtocolConstants.CURSOR_TYPE_READ_ONLY);
        } else {
            packet.writeByte(ProtocolConstants.CURSOR_TYPE_NO_CURSOR);
        }
        PacketUtils.writeInt4(packet, 1);//4. iteration_count,Number of times to execute the statement. Currently always 1.

        return packet;
    }

    private MySQLType decideBindByte(int stmtIndex, MySQLColumnMeta meta, ParamValue paramValue) {
        final MySQLType mySQLType;
        switch (meta.mysqlType) {
            case BIT: {
                if ((meta.length & 7) == 0) {
                    mySQLType = MySQLType.BIT;
                } else {
                    mySQLType = decideBitBindType(stmtIndex, paramValue);
                }
            }
            break;
            case FLOAT_UNSIGNED:
                mySQLType = MySQLType.DOUBLE;
                break;
            case DOUBLE_UNSIGNED:
                mySQLType = MySQLType.DECIMAL;
                break;
            default:
                mySQLType = meta.mysqlType;
        }
        return mySQLType;
    }


    /**
     * @see #createExecutePacketBuffer(int)
     */
    private void bindParameter(ByteBuf buffer, int stmtIndex, MySQLColumnMeta meta
            , ParamValue paramValue)
            throws SQLException {

        switch (decideBindByte(stmtIndex, meta, paramValue)) {
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT:
            case INT_UNSIGNED:
                bindToInt4(buffer, stmtIndex, meta, paramValue);
                break;
            case BIGINT:
            case BIGINT_UNSIGNED:
                bindToInt8(buffer, stmtIndex, meta, paramValue);
                break;
            case FLOAT:
            case FLOAT_UNSIGNED:
                bindToFloat(buffer, stmtIndex, meta, paramValue);
                break;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                bindToDouble(buffer, stmtIndex, meta, paramValue);
                break;
            case BIT:
                bindToBit(buffer, stmtIndex, meta, paramValue);
                break;
            case BOOLEAN:
            case TINYINT:
            case TINYINT_UNSIGNED:
                bindToInt1(buffer, stmtIndex, meta, paramValue);
                break;
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case YEAR:
                bindInt2(buffer, stmtIndex, meta, paramValue);
                break;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                bindToDecimal(buffer, stmtIndex, meta, paramValue);
                break;
            case ENUM:
            case VARCHAR:
            case CHAR:
            case JSON:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                // below binary
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB:
            case GEOMETRY:
                bindToStringType(buffer, stmtIndex, meta.mysqlType, paramValue);
                break;
            case SET:
                bindToSetType(buffer, stmtIndex, meta, paramValue);
                break;
            case TIME:
                bindToTime(buffer, stmtIndex, meta, paramValue);
                break;
            case DATE:
                bindToDate(buffer, stmtIndex, meta, paramValue);
                break;
            case DATETIME:
            case TIMESTAMP:
                bindToDatetime(buffer, stmtIndex, meta, paramValue);
                break;
            case NULL:
            case UNKNOWN:
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, meta.mysqlType, paramValue);
            default:
                throw MySQLExceptions.createUnknownEnumException(meta.mysqlType);
        }
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToInt1(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNullValue();
        final int int1;
        final int unsignedMaxByte = Byte.toUnsignedInt((byte) -1);
        if (nonNullValue instanceof Byte) {
            byte num = (Byte) nonNullValue;
            if (parameterMeta.mysqlType.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, parameterMeta.mysqlType, bindValue
                        , 0, unsignedMaxByte);
            }
            int1 = num;
        } else if (nonNullValue instanceof Boolean) {
            int1 = (Boolean) nonNullValue ? 1 : 0;
        } else if (nonNullValue instanceof Integer
                || nonNullValue instanceof Long
                || nonNullValue instanceof Short) {
            int1 = longTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType, ((Number) nonNullValue).longValue());
        } else if (nonNullValue instanceof String) {
            Number num;
            try {
                num = longTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType, Integer.parseInt((String) nonNullValue));
            } catch (NumberFormatException e) {
                try {
                    num = bigIntegerTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType
                            , new BigInteger((String) nonNullValue));
                } catch (NumberFormatException ne) {
                    throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
                }
            }
            int1 = num.intValue();
        } else if (nonNullValue instanceof BigInteger) {
            int1 = bigIntegerTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw MySQLExceptions.createNotSupportScaleException(stmtIndex, parameterMeta.mysqlType, bindValue);
            } else {
                int1 = bigIntegerTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType, num.toBigInteger());
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
        }

        PacketUtils.writeInt1(buffer, int1);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindInt2(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNullValue();

        final int unsignedMaxShort = Short.toUnsignedInt((short) -1);
        final int int2;
        if (nonNullValue instanceof Year) {
            int2 = ((Year) nonNullValue).getValue();
        } else if (nonNullValue instanceof Short) {
            short num = (Short) nonNullValue;
            if (parameterMeta.mysqlType.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, parameterMeta.mysqlType, bindValue
                        , 0, unsignedMaxShort);
            }
            int2 = num;
        } else if (nonNullValue instanceof Integer
                || nonNullValue instanceof Byte
                || nonNullValue instanceof Long) {
            int2 = longTotInt2(bindValue, stmtIndex, parameterMeta.mysqlType, ((Number) nonNullValue).longValue());
        } else if (nonNullValue instanceof String) {
            Number num;
            try {
                num = longTotInt2(bindValue, stmtIndex, parameterMeta.mysqlType
                        , Integer.parseInt((String) nonNullValue));
            } catch (NumberFormatException e) {
                try {
                    num = bigIntegerTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType
                            , new BigInteger((String) nonNullValue));
                } catch (NumberFormatException ne) {
                    throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
                }
            }
            int2 = num.intValue();
        } else if (nonNullValue instanceof BigInteger) {
            int2 = bigIntegerTotInt2(bindValue, stmtIndex, parameterMeta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw MySQLExceptions.createNotSupportScaleException(stmtIndex, parameterMeta.mysqlType, bindValue);
            } else {
                int2 = bigIntegerTotInt2(bindValue, stmtIndex, parameterMeta.mysqlType, num.toBigInteger());
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
        }
        PacketUtils.writeInt2(buffer, int2);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value">Binary Protocol Value</a>
     */
    private void bindToDecimal(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue paramValue) {
        final Object nonNullValue = paramValue.getNonNullValue();
        final String decimal;
        if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            decimal = num.toPlainString();
        } else if (nonNullValue instanceof BigInteger) {
            BigInteger num = (BigInteger) nonNullValue;
            decimal = num.toString();
        } else if (nonNullValue instanceof String) {
            decimal = (String) nonNullValue;
        } else if (nonNullValue instanceof byte[]) {
            PacketUtils.writeStringLenEnc(buffer, (byte[]) nonNullValue);
            return;
        } else if (nonNullValue instanceof Integer
                || nonNullValue instanceof Long
                || nonNullValue instanceof Short
                || nonNullValue instanceof Byte) {
            decimal = BigInteger.valueOf(((Number) nonNullValue).longValue()).toString();
        } else if (nonNullValue instanceof Double || nonNullValue instanceof Float) {
            decimal = nonNullValue.toString();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, paramValue);
        }
        PacketUtils.writeStringLenEnc(buffer, decimal.getBytes(this.adjutant.obtainCharsetClient()));
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value">Binary Protocol Value</a>
     */
    private void bindToInt4(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue paramValue) {
        final Object nonNullValue = paramValue.getNonNullValue();
        final long unsignedMaxInt = Integer.toUnsignedLong(-1);
        final int int4;
        if (nonNullValue instanceof Integer) {
            int num = (Integer) nonNullValue;
            if (meta.mysqlType.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                        , 0, unsignedMaxInt);
            }
            int4 = num;
        } else if (nonNullValue instanceof Long) {
            int4 = longToInt4(stmtIndex, paramValue, meta.mysqlType, (Long) nonNullValue);
        } else if (nonNullValue instanceof String) {
            int num;
            try {
                num = longToInt4(stmtIndex, paramValue, meta.mysqlType, Long.parseLong((String) nonNullValue));
            } catch (NumberFormatException e) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                        , 0, unsignedMaxInt);

            }
            int4 = num;
        } else if (nonNullValue instanceof BigInteger) {
            int4 = bigIntegerToIn4(stmtIndex, paramValue, meta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof Short) {
            short num = ((Short) nonNullValue);
            if (meta.mysqlType.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                        , 0, unsignedMaxInt);
            }
            int4 = num;
        } else if (nonNullValue instanceof Byte) {
            byte num = ((Byte) nonNullValue);
            if (meta.mysqlType.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                        , 0, unsignedMaxInt);
            }
            int4 = num;
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw MySQLExceptions.createNotSupportScaleException(stmtIndex, meta.mysqlType, paramValue);
            } else if (meta.mysqlType.isUnsigned()) {
                if (num.compareTo(BigDecimal.ZERO) < 0
                        || num.compareTo(BigDecimal.valueOf(unsignedMaxInt)) > 0) {
                    throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                            , 0, unsignedMaxInt);
                }
            } else if (num.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0
                    || num.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                        , 0, unsignedMaxInt);
            }
            int4 = num.intValue();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, paramValue);
        }

        PacketUtils.writeInt4(buffer, int4);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToFloat(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNullValue();
        final float floatValue;
        if (nonNullValue instanceof Float) {
            floatValue = (Float) nonNullValue;
        } else if (nonNullValue instanceof String) {
            try {
                floatValue = Float.parseFloat((String) nonNullValue);
            } catch (NumberFormatException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
            }
        } else if (nonNullValue instanceof Short) {
            floatValue = ((Short) nonNullValue).floatValue();
        } else if (nonNullValue instanceof Byte) {
            floatValue = ((Byte) nonNullValue).floatValue();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
        }

        PacketUtils.writeInt4(buffer, Float.floatToIntBits(floatValue));
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToInt8(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNullValue();
        final long int8;
        if (nonNullValue instanceof Long) {
            long num = (Long) nonNullValue;
            if (meta.mysqlType.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, bindValue
                        , 0, MySQLNumberUtils.MAX_UNSIGNED_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof BigInteger) {
            int8 = bigIntegerToInt8(bindValue, stmtIndex, meta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof Integer) {
            int num = (Integer) nonNullValue;
            if (meta.mysqlType.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, bindValue
                        , 0, MySQLNumberUtils.MAX_UNSIGNED_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof Short) {
            int num = (Short) nonNullValue;
            if (meta.mysqlType.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, bindValue
                        , 0, MySQLNumberUtils.MAX_UNSIGNED_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof Byte) {
            int num = (Byte) nonNullValue;
            if (meta.mysqlType.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, bindValue
                        , 0, MySQLNumberUtils.MAX_UNSIGNED_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof String) {
            try {
                BigInteger big = new BigInteger((String) nonNullValue);
                int8 = bigIntegerToInt8(bindValue, stmtIndex, meta.mysqlType, big);
            } catch (NumberFormatException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
            }
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw MySQLExceptions.createNotSupportScaleException(stmtIndex, meta.mysqlType, bindValue);
            } else {
                int8 = bigIntegerToInt8(bindValue, stmtIndex, meta.mysqlType, num.toBigInteger());
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
        }

        PacketUtils.writeInt8(buffer, int8);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToDouble(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNullValue();
        final double value;
        if (nonNullValue instanceof Double) {
            value = (Double) nonNullValue;
        } else if (nonNullValue instanceof Float) {
            value = (Float) nonNullValue;
        } else if (nonNullValue instanceof String) {
            try {
                value = Double.parseDouble((String) nonNullValue);
            } catch (NumberFormatException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
            }
        } else if (nonNullValue instanceof Integer) {
            value = ((Integer) nonNullValue).doubleValue();
        } else if (nonNullValue instanceof Short) {
            value = ((Short) nonNullValue).doubleValue();
        } else if (nonNullValue instanceof Byte) {
            value = ((Byte) nonNullValue).doubleValue();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
        }

        PacketUtils.writeInt8(buffer, Double.doubleToLongBits(value));
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToTime(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNullValue();

        final int microPrecision = parameterMeta.obtainDateTimeTypePrecision();
        final int length = microPrecision > 0 ? 12 : 8;

        if (nonNullValue instanceof Duration) {
            Duration duration = (Duration) nonNullValue;
            buffer.writeByte(length); //1. length

            buffer.writeByte(duration.isNegative() ? 1 : 0); //2. is_negative
            duration = duration.abs();
            if (!MySQLTimeUtils.canConvertToTimeType(duration)) {
                throw MySQLExceptions.createDurationRangeException(stmtIndex, parameterMeta.mysqlType, bindValue);
            }
            long temp;
            temp = duration.toDays();
            PacketUtils.writeInt4(buffer, (int) temp); //3. days
            duration = duration.minusDays(temp);

            temp = duration.toHours();
            buffer.writeByte((int) temp); //4. hour
            duration = duration.minusHours(temp);

            temp = duration.toMinutes();
            buffer.writeByte((int) temp); //5. minute
            duration = duration.minusMinutes(temp);

            temp = duration.getSeconds();
            buffer.writeByte((int) temp); //6. second
            duration = duration.minusSeconds(temp);
            if (length == 12) {
                //7, micro seconds
                PacketUtils.writeInt4(buffer, truncateMicroSeconds((int) duration.toMillis(), microPrecision));
            }
            return;
        }

        final LocalTime time;
        if (nonNullValue instanceof LocalTime) {
            time = OffsetTime.of((LocalTime) nonNullValue, this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime();
        } else if (nonNullValue instanceof OffsetTime) {
            time = ((OffsetTime) nonNullValue).withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime();
        } else if (nonNullValue instanceof String) {
            String timeText = (String) nonNullValue;
            try {
                time = OffsetTime.of(LocalTime.parse(timeText, MySQLTimeUtils.MYSQL_TIME_FORMATTER)
                        , this.adjutant.obtainZoneOffsetClient())
                        .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                        .toLocalTime();
            } catch (DateTimeParseException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue, e);
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
        }
        if (time != null) {
            buffer.writeByte(length); //1. length
            buffer.writeByte(0); //2. is_negative
            buffer.writeZero(4); //3. days

            buffer.writeByte(time.getHour()); //4. hour
            buffer.writeByte(time.getMinute()); //5. minute
            buffer.writeByte(time.getSecond()); ///6. second

            if (length == 12) {
                //7, micro seconds
                PacketUtils.writeInt4(buffer
                        , truncateMicroSeconds(time.get(ChronoField.MICRO_OF_SECOND), microPrecision));
            }
        }

    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToDate(final ByteBuf buffer, int stmtIndex, MySQLColumnMeta columnMeta, ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNullValue();

        final LocalDate date;
        if (nonNullValue instanceof LocalDate) {
            date = (LocalDate) nonNullValue;

        } else if (nonNullValue instanceof String) {
            try {
                date = LocalDate.parse((String) nonNullValue);
            } catch (DateTimeParseException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, columnMeta.mysqlType, bindValue, e);
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, columnMeta.mysqlType, bindValue);
        }
        buffer.writeByte(4); // length
        PacketUtils.writeInt2(buffer, date.getYear()); // year
        buffer.writeByte(date.getMonthValue()); // month
        buffer.writeByte(date.getDayOfMonth()); // day
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToDatetime(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNullValue();

        final LocalDateTime dateTime;
        if (nonNullValue instanceof LocalDateTime) {
            dateTime = OffsetDateTime.of((LocalDateTime) nonNullValue, this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else if (nonNullValue instanceof ZonedDateTime) {
            dateTime = ((ZonedDateTime) nonNullValue)
                    .withZoneSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else if (nonNullValue instanceof OffsetDateTime) {
            dateTime = ((OffsetDateTime) nonNullValue)
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else if (nonNullValue instanceof String) {
            try {
                LocalDateTime localDateTime = LocalDateTime.parse((String) nonNullValue
                        , MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
                dateTime = OffsetDateTime.of(localDateTime, this.adjutant.obtainZoneOffsetClient())
                        .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                        .toLocalDateTime();
            } catch (DateTimeParseException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue, e);
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
        }

        final int microPrecision = parameterMeta.obtainDateTimeTypePrecision();
        buffer.writeByte(microPrecision > 0 ? 11 : 7); // length
        PacketUtils.writeInt2(buffer, dateTime.getYear()); // year
        buffer.writeByte(dateTime.getMonthValue()); // month
        buffer.writeByte(dateTime.getDayOfMonth()); // day

        buffer.writeByte(dateTime.getHour()); // hour
        buffer.writeByte(dateTime.getMinute()); // minute
        buffer.writeByte(dateTime.getSecond()); // second

        if (microPrecision > 0) {
            // micro second
            PacketUtils.writeInt4(buffer
                    , truncateMicroSeconds(dateTime.get(ChronoField.MICRO_OF_SECOND), microPrecision));
        }

    }


    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToBit(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue bindValue) throws JdbdSQLException {
        final long bitNumber = BindUtils.bindToBits(stmtIndex, meta.mysqlType, bindValue
                , this.adjutant.obtainCharsetClient());

        final int length = (int) meta.length;
        if ((length & 7) != 0) {
            // MySQL server can't parse.
            throw new IllegalStateException("Invoker bug.");
        }
        final long maxBits = (1L << length) - 1L;

        if ((bitNumber & (~maxBits)) != 0) {
            throw MySQLExceptions.createDataTooLongException(stmtIndex, meta.mysqlType, bindValue);
        }
        final byte[] bytes = new byte[length >>> 3];

        MySQLNumberUtils.longToBigEndian(bitNumber, bytes, 0, bytes.length);
        PacketUtils.writeStringLenEnc(buffer, bytes);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToStringType(final ByteBuf buffer, final int stmtIndex, final MySQLType mySQLType
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNullValue();
        final Charset charset = this.adjutant.obtainCharsetClient();
        if (nonNullValue instanceof CharSequence || nonNullValue instanceof Character) {
            PacketUtils.writeStringLenEnc(buffer, nonNullValue.toString().getBytes(charset));
        } else if (nonNullValue instanceof byte[]) {
            PacketUtils.writeStringLenEnc(buffer, (byte[]) nonNullValue);
        } else if (nonNullValue instanceof Enum) {
            PacketUtils.writeStringLenEnc(buffer, ((Enum<?>) nonNullValue).name().getBytes(charset));
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, mySQLType, bindValue);
        }

    }

    private void bindToSetType(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNullValue();
        final String text;
        if (nonNullValue instanceof String) {
            text = (String) nonNullValue;
        } else if (nonNullValue instanceof Set) {
            Set<?> set = (Set<?>) nonNullValue;
            StringBuilder builder = new StringBuilder(set.size() * 6);
            int index = 0;
            for (Object o : set) {
                if (index > 0) {
                    builder.append(",");
                }
                if (o instanceof String) {
                    builder.append((String) o);
                } else if (o instanceof Enum) {
                    builder.append(((Enum<?>) o).name());
                } else {
                    throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
                }
                index++;
            }
            text = builder.toString();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
        }
        PacketUtils.writeStringLenEnc(buffer, text.getBytes(this.adjutant.obtainCharsetClient()));
    }


    /*################################## blow private static method ##################################*/

    /**
     * @see #bindToTime(ByteBuf, int, MySQLColumnMeta, ParamValue)
     * @see #bindToDatetime(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private static int truncateMicroSeconds(final int microSeconds, final int precision) {
        final int newMicroSeconds;
        switch (precision) {
            case 0:
                newMicroSeconds = 0;
                break;
            case 1:
                newMicroSeconds = (microSeconds / 100000) * 100000;
                break;
            case 2:
                newMicroSeconds = (microSeconds / 10000) * 10000;
                break;
            case 3:
                newMicroSeconds = (microSeconds / 1000) * 1000;
                break;
            case 4:
                newMicroSeconds = (microSeconds / 100) * 100;
                break;
            case 5:
                newMicroSeconds = (microSeconds / 10) * 10;
                break;
            case 6:
                newMicroSeconds = microSeconds;
                break;
            default:
                throw new IllegalArgumentException(String.format("precision[%s] not in [0,6]", precision));
        }
        return newMicroSeconds;
    }


    /*################################## blow private static convert method ##################################*/

    /**
     * @see #bindToInt4(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private static int longToInt4(int stmtIndex, ParamValue bindValue, MySQLType mySQLType, final long num) {
        if (mySQLType.isUnsigned()) {
            final long unsignedMaxInt = Integer.toUnsignedLong(-1);
            if (num < 0 || num > unsignedMaxInt) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                        , 0, unsignedMaxInt);
            }
        } else if (num < Integer.MIN_VALUE || num > Integer.MAX_VALUE) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        return (int) num;
    }

    /**
     * @see #bindToInt4(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private static int bigIntegerToIn4(int stmtIndex, ParamValue paramValue, MySQLType mySQLType
            , final BigInteger num) {
        if (mySQLType.isUnsigned()) {
            BigInteger unsignedMaxInt = BigInteger.valueOf(Integer.toUnsignedLong(-1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxInt) > 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, paramValue
                        , 0, unsignedMaxInt);
            }
        } else if (num.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, paramValue
                    , Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        return num.intValue();
    }

    /**
     * @see #bindToInt8(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private static long bigIntegerToInt8(ParamValue bindValue, int stmtIndex, MySQLType mySQLType
            , final BigInteger num) {
        if (mySQLType.isUnsigned()) {
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(MySQLNumberUtils.MAX_UNSIGNED_LONG) > 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                        , 0, MySQLNumberUtils.MAX_UNSIGNED_LONG);
            }
        } else if (num.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Long.MIN_VALUE, Long.MAX_VALUE);
        }
        return num.longValue();
    }

    private static int longTotInt1(ParamValue bindValue, int stmtIndex, MySQLType mySQLType, final long num) {

        if (mySQLType.isUnsigned()) {
            int unsignedMaxByte = Byte.toUnsignedInt((byte) -1);
            if (num < 0 || num > unsignedMaxByte) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue, 0, unsignedMaxByte);
            }
        } else if (num < Byte.MIN_VALUE || num > Byte.MAX_VALUE) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        return (int) num;
    }

    private static int longTotInt2(ParamValue bindValue, int stmtIndex, MySQLType mySQLType, final long num) {

        if (mySQLType.isUnsigned()) {
            int unsignedMaxShort = Short.toUnsignedInt((short) -1);
            if (num < 0 || num > unsignedMaxShort) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                        , 0, unsignedMaxShort);
            }
        } else if (num < Short.MIN_VALUE || num > Short.MAX_VALUE) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Short.MIN_VALUE, Short.MAX_VALUE);
        }
        return (int) num;
    }

    private static int bigIntegerTotInt1(ParamValue bindValue, int stmtIndex, MySQLType mySQLType
            , final BigInteger num) {

        if (mySQLType.isUnsigned()) {
            BigInteger unsignedMaxByte = BigInteger.valueOf(Byte.toUnsignedInt((byte) -1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxByte) > 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                        , 0, unsignedMaxByte);
            }
        } else if (num.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) > 0) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        return num.intValue();
    }

    private static int bigIntegerTotInt2(ParamValue bindValue, int stmtIndex, MySQLType mySQLType
            , final BigInteger num) {

        if (mySQLType.isUnsigned()) {
            BigInteger unsignedMaxInt2 = BigInteger.valueOf(Short.toUnsignedInt((byte) -1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxInt2) > 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue, 0, unsignedMaxInt2);
            }
        } else if (num.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) > 0) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Short.MIN_VALUE, Short.MAX_VALUE);
        }
        return num.intValue();
    }


}
