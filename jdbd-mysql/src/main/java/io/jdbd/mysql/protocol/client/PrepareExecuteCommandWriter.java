package io.jdbd.mysql.protocol.client;

import io.jdbd.BindParameterException;
import io.jdbd.SQLBindParameterException;
import io.jdbd.mysql.BindValue;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.mysql.util.MySQLNumberUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.jdbd.type.Geometry;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * @see ComPreparedTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
 */
final class PrepareExecuteCommandWriter implements StatementCommandWriter {

    private final StatementTask statementTask;

    private final int statementId;

    private final MySQLColumnMeta[] paramMetaArray;

    private final boolean query;

    private final ClientProtocolAdjutant adjutant;

    private final Properties properties;

    private final LongParameterWriter longParameterWriter;

    private final boolean fetchResultSet;


    PrepareExecuteCommandWriter(final StatementTask statementTask) {
        this.statementTask = statementTask;
        this.statementId = statementTask.obtainStatementId();
        this.paramMetaArray = statementTask.obtainParameterMetas();
        this.query = statementTask.returnResultSet();

        this.adjutant = statementTask.obtainAdjutant();
        this.properties = this.adjutant.obtainHostInfo().getProperties();
        this.longParameterWriter = new PrepareLongParameterWriter(statementTask);
        this.fetchResultSet = statementTask.isFetchResult();
    }


    @Override
    public Publisher<ByteBuf> writeCommand(final List<BindValue> parameterGroup) {
        final int size = parameterGroup.size();
        final MySQLColumnMeta[] paramMetaArray = this.paramMetaArray;
        if (size != paramMetaArray.length) {
            throw new SQLBindParameterException(
                    String.format("Bind parameter size[%s] and sql parameter size[%s] not match."
                            , size, paramMetaArray.length));
        }
        List<BindValue> longParamList = null;

        for (int i = 0; i < size; i++) {
            BindValue bindValue = parameterGroup.get(i);
            if (bindValue.getParamIndex() != i) {
                throw new IllegalArgumentException(
                        String.format("parameterGroup BindValue parameter index[%s] and position[%s] not match."
                                , bindValue.getParamIndex(), i));

            } else if (bindValue.getType() != paramMetaArray[i].mysqlType) {
                throw new IllegalArgumentException(
                        String.format("BindValue parameter index[%s] SQLType[%s] and parameter type[%s] not match."
                                , bindValue.getParamIndex(), bindValue.getType(), paramMetaArray[i].mysqlType));
            } else if (bindValue.isLongData()) {
                if (longParamList == null) {
                    longParamList = new ArrayList<>();
                }
                longParamList.add(bindValue);
            }

        }

        final Publisher<ByteBuf> publisher;
        if (size == 0) {
            // this 'if' block handle no bind parameter.
            ByteBuf packet = createExecutePacketBuffer(10);
            PacketUtils.writePacketHeader(packet, this.statementTask.addAndGetSequenceId());
            publisher = Mono.just(packet);
        } else if (longParamList == null) {
            // this 'if' block handle no long parameter.
            publisher = createExecutionPacketPublisher(parameterGroup);
        } else {
            publisher = this.longParameterWriter.write(Collections.unmodifiableList(longParamList))
                    .concatWith(Flux.defer(() -> createExecutionPacketPublisher(parameterGroup)));
        }
        return publisher;
    }

    /*################################## blow private method ##################################*/


    /**
     * @see #writeCommand(List)
     */
    private Flux<ByteBuf> createExecutionPacketPublisher(final List<BindValue> parameterGroup) {
        return Flux.create(sink -> emitExecutionPackets(parameterGroup, sink));
    }

    /**
     * @see #createExecutionPacketPublisher(List)
     */
    private void emitExecutionPackets(final List<BindValue> parameterGroup, final FluxSink<ByteBuf> sink) {
        final MySQLColumnMeta[] parameterMetaArray = this.paramMetaArray;
        final byte[] nullBitsMap = new byte[(parameterMetaArray.length + 7) / 8];

        final int parameterCount = parameterGroup.size();
        //1. make nullBitsMap and parameterValueLength
        long parameterValueLength = 0L;
        int i = 0;
        try {
            for (; i < parameterCount; i++) {
                BindValue bindValue = parameterGroup.get(i);
                if (bindValue.getValue() == null || bindValue.getType() == MySQLType.NULL) {
                    nullBitsMap[i / 8] |= (1 << (i & 7));
                } else if (!bindValue.isLongData()) {
                    parameterValueLength += obtainParameterValueLength(parameterMetaArray[i], bindValue);
                }
            }
            i = 0;
        } catch (Exception e) {
            sink.error(new BindParameterException(String.format("Parameter[%s] type not compatibility", i), i));
            return;
        }

        final int prefixLength = 10 + nullBitsMap.length + 1 + (parameterCount << 1);
        final ByteBuf packetBuffer;
        if ((PacketUtils.HEADER_SIZE + prefixLength + parameterValueLength) <= Integer.MAX_VALUE) {
            packetBuffer = createExecutePacketBuffer(prefixLength + (int) parameterValueLength);
        } else {
            sink.error(new SQLBindParameterException(
                    "Bind parameter too long,execute packet great than Integer.MAX_VALUE,please use %s or %s"
                    , InputStream.class.getName(), Reader.class.getName()));
            return;
        }
        packetBuffer.writeBytes(nullBitsMap); //fill null_bitmap
        packetBuffer.writeByte(1); //fill new_params_bind_flag
        //fill  parameter_types
        for (BindValue value : parameterGroup) {
            PacketUtils.writeInt2(packetBuffer, value.getType().parameterType);
        }
        //fill parameter_values
        try {

            for (i = 0; i < parameterCount; i++) {
                BindValue bindValue = parameterGroup.get(i);
                if (bindValue.isLongData() || bindValue.getValue() == null) {
                    continue;
                }
                // bind parameter bto packet buffer
                bindParameter(packetBuffer, parameterMetaArray[i], bindValue);

            }
            PacketUtils.publishBigPacket(packetBuffer, sink, this.statementTask::addAndGetSequenceId
                    , this.adjutant.alloc()::buffer, true);
        } catch (Throwable e) {
            packetBuffer.release();
            sink.error(new BindParameterException(String.format("Bind parameter[%s] write error.", i), e, i));
        }
    }

    /**
     * @see #createExecutionPacketPublisher(List)
     */
    private ByteBuf createExecutePacketBuffer(int initialPayloadCapacity) {

        ByteBuf packetBuffer = this.adjutant.alloc().buffer(initialPayloadCapacity, Integer.MAX_VALUE);

        packetBuffer.writeByte(PacketUtils.COM_STMT_EXECUTE); // 1.status
        PacketUtils.writeInt4(packetBuffer, this.statementId);// 2. statement_id
        //3.cursor Flags, reactive api not support cursor
        if (this.fetchResultSet) {
            packetBuffer.writeByte(ProtocolConstants.CURSOR_TYPE_READ_ONLY);
        } else {
            packetBuffer.writeByte(ProtocolConstants.CURSOR_TYPE_NO_CURSOR);

        }
        PacketUtils.writeInt4(packetBuffer, 1);//4. iteration_count,Number of times to execute the statement. Currently always 1.

        return packetBuffer;
    }

    /**
     * @return parameter value byte length ,if return {@link Integer#MIN_VALUE} ,then parameter error,should end task.
     * @throws IllegalArgumentException when {@link BindValue#getValue()} is null.
     * @see #emitExecutionPackets(List, FluxSink)
     */
    private long obtainParameterValueLength(MySQLColumnMeta parameterMeta, BindValue bindValue) {
        final Object value = bindValue.getRequiredValue();
        final long length;
        switch (bindValue.getType()) {
            case INT:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                length = 4L;
                break;
            case DATE:
                length = 5L;
                break;
            case BIGINT:
            case INT_UNSIGNED:
            case BIGINT_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case BIT:
                length = 8L;
                break;
            case BOOLEAN:
            case TINYINT:
            case TINYINT_UNSIGNED:
                length = 1L;
                break;
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case YEAR:
                length = 2L;
                break;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                length = parameterMeta.length;
                break;
            case VARCHAR:
            case CHAR:
            case SET:
            case JSON:
            case ENUM:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB: {
                long lenEncBytes;
                if (value instanceof String) {
                    lenEncBytes = (long) ((String) value).length() * this.adjutant.obtainMaxBytesPerCharClient();
                } else if (value instanceof byte[]) {
                    lenEncBytes = ((byte[]) value).length;
                } else {
                    String m = String.format("Bind parameter[%s] not support type[%s]"
                            , bindValue.getParamIndex(), value.getClass().getName());
                    throw new BindParameterException(m, bindValue.getParamIndex());
                }
                length = PacketUtils.obtainIntLenEncLength(lenEncBytes) + lenEncBytes;
            }
            break;
            case DATETIME:
            case TIMESTAMP:
                length = (parameterMeta.decimals > 0 && parameterMeta.decimals < 7) ? 11 : 7;
                break;
            case TIME:
                length = (parameterMeta.decimals > 0 && parameterMeta.decimals < 7) ? 12 : 8;
                break;
            case NULL:
            case UNKNOWN:
            case GEOMETRY:
                length = 0; // TODO 优化
                break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(parameterMeta.mysqlType);
        }

        return length;
    }

    /**
     * @see #emitExecutionPackets(List, FluxSink)
     */
    private void bindParameter(ByteBuf buffer, MySQLColumnMeta parameterMeta, BindValue bindValue) {

        switch (parameterMeta.mysqlType) {
            case INT:
            case MEDIUMINT:
            case INT_UNSIGNED:
            case MEDIUMINT_UNSIGNED:
                bindToInt4(buffer, parameterMeta, bindValue);
                break;
            case BIGINT:
            case BIGINT_UNSIGNED:
                bindToInt8(buffer, parameterMeta, bindValue);
                break;
            case FLOAT:
            case FLOAT_UNSIGNED:
                bindToFloat(buffer, bindValue);
                break;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                bindToDouble(buffer, bindValue);
                break;
            case BIT:
                bindToBit(buffer, parameterMeta, bindValue);
                break;
            case BOOLEAN:
            case TINYINT:
            case TINYINT_UNSIGNED:
                bindToInt1(buffer, parameterMeta, bindValue);
                break;
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case YEAR:
                bindInt2(buffer, parameterMeta, bindValue);
                break;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                bindToDecimal(buffer, bindValue);
                break;
            case VARCHAR:
            case CHAR:
            case SET:
            case JSON:
            case ENUM:
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
                bindToStringType(buffer, bindValue);
                break;
            case TIME:
                bindToTime(buffer, parameterMeta, bindValue);
                break;
            case DATE:
                bindToDate(buffer, bindValue);
                break;
            case DATETIME:
            case TIMESTAMP:
                bindToDatetime(buffer, parameterMeta, bindValue);
                break;
            case UNKNOWN:
            case GEOMETRY:
                //TODO add code
                throw BindUtils.createTypeNotMatchException(bindValue);
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(parameterMeta.mysqlType);
        }
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToInt1(final ByteBuf buffer, final MySQLColumnMeta parameterMeta, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final int int1;
        final int unsignedMaxByte = Byte.toUnsignedInt((byte) -1);
        if (nonNullValue instanceof Byte) {
            byte num = (Byte) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxByte);
            }
            int1 = num;
        } else if (nonNullValue instanceof Boolean) {
            int1 = (Boolean) nonNullValue ? 1 : 0;
        } else if (nonNullValue instanceof Integer
                || nonNullValue instanceof Long
                || nonNullValue instanceof Short) {
            int1 = longTotInt1(bindValue, parameterMeta.mysqlType, ((Number) nonNullValue).longValue());
        } else if (nonNullValue instanceof String) {
            Number num;
            try {
                num = longTotInt1(bindValue, parameterMeta.mysqlType, Integer.parseInt((String) nonNullValue));
            } catch (NumberFormatException e) {
                try {
                    num = bigIntegerTotInt1(bindValue, parameterMeta.mysqlType, new BigInteger((String) nonNullValue));
                } catch (NumberFormatException ne) {
                    throw BindUtils.createTypeNotMatchException(bindValue);
                }
            }
            int1 = num.intValue();
        } else if (nonNullValue instanceof BigInteger) {
            int1 = bigIntegerTotInt1(bindValue, parameterMeta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw BindUtils.createNotSupportFractionException(bindValue);
            } else {
                int1 = bigIntegerTotInt1(bindValue, parameterMeta.mysqlType, num.toBigInteger());
            }
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }

        PacketUtils.writeInt1(buffer, int1);
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindInt2(final ByteBuf buffer, final MySQLColumnMeta parameterMeta, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();

        final int unsignedMaxShort = Short.toUnsignedInt((short) -1);
        final int int2;
        if (nonNullValue instanceof Year) {
            int2 = ((Year) nonNullValue).getValue();
        } else if (nonNullValue instanceof Short) {
            short num = (Short) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxShort);
            }
            int2 = num;
        } else if (nonNullValue instanceof Integer
                || nonNullValue instanceof Byte
                || nonNullValue instanceof Long) {
            int2 = longTotInt2(bindValue, parameterMeta.mysqlType, ((Number) nonNullValue).longValue());
        } else if (nonNullValue instanceof String) {
            Number num;
            try {
                num = longTotInt2(bindValue, parameterMeta.mysqlType, Integer.parseInt((String) nonNullValue));
            } catch (NumberFormatException e) {
                try {
                    num = bigIntegerTotInt1(bindValue, parameterMeta.mysqlType, new BigInteger((String) nonNullValue));
                } catch (NumberFormatException ne) {
                    throw BindUtils.createTypeNotMatchException(bindValue);
                }
            }
            int2 = num.intValue();
        } else if (nonNullValue instanceof BigInteger) {
            int2 = bigIntegerTotInt2(bindValue, parameterMeta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw BindUtils.createNotSupportFractionException(bindValue);
            } else {
                int2 = bigIntegerTotInt2(bindValue, parameterMeta.mysqlType, num.toBigInteger());
            }
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }
        PacketUtils.writeInt2(buffer, int2);
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToDecimal(final ByteBuf buffer, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
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
            throw BindUtils.createTypeNotMatchException(bindValue);
        }
        PacketUtils.writeStringLenEnc(buffer, decimal.getBytes(this.adjutant.obtainCharsetClient()));
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToInt4(final ByteBuf buffer, final MySQLColumnMeta parameterMeta, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final long unsignedMaxInt = Integer.toUnsignedLong(-1);
        final int int4;
        if (nonNullValue instanceof Integer) {
            int num = (Integer) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxInt);
            }
            int4 = num;
        } else if (nonNullValue instanceof Long) {
            int4 = longToInt4(bindValue, parameterMeta.mysqlType, (Long) nonNullValue);
        } else if (nonNullValue instanceof String) {
            int num;
            try {
                num = longToInt4(bindValue, parameterMeta.mysqlType, Long.parseLong((String) nonNullValue));
            } catch (NumberFormatException e) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxInt);
            }
            int4 = num;
        } else if (nonNullValue instanceof BigInteger) {
            int4 = bigIntegerToIn4(bindValue, parameterMeta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof Short) {
            short num = ((Short) nonNullValue);
            if (parameterMeta.isUnsigned() && num < 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxInt);
            }
            int4 = num;
        } else if (nonNullValue instanceof Byte) {
            byte num = ((Byte) nonNullValue);
            if (parameterMeta.isUnsigned() && num < 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxInt);
            }
            int4 = num;
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw BindUtils.createNotSupportFractionException(bindValue);
            } else if (parameterMeta.isUnsigned()) {
                if (num.compareTo(BigDecimal.ZERO) < 0
                        || num.compareTo(BigDecimal.valueOf(unsignedMaxInt)) > 0) {
                    throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxInt);
                }
            } else if (num.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0
                    || num.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxInt);
            }
            int4 = num.intValue();
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }

        PacketUtils.writeInt4(buffer, int4);
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToFloat(final ByteBuf buffer, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final float floatValue;
        if (nonNullValue instanceof Float) {
            floatValue = (Float) nonNullValue;
        } else if (nonNullValue instanceof String) {
            try {
                floatValue = Float.parseFloat((String) nonNullValue);
            } catch (NumberFormatException e) {
                throw BindUtils.createTypeNotMatchException(bindValue);
            }
        } else if (nonNullValue instanceof Short) {
            floatValue = ((Short) nonNullValue).floatValue();
        } else if (nonNullValue instanceof Byte) {
            floatValue = ((Byte) nonNullValue).floatValue();
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }

        PacketUtils.writeInt4(buffer, Float.floatToIntBits(floatValue));
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToInt8(final ByteBuf buffer, final MySQLColumnMeta parameterMeta, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final long int8;
        if (nonNullValue instanceof Long) {
            long num = (Long) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, MySQLNumberUtils.UNSIGNED_MAX_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof BigInteger) {
            int8 = bigIntegerToInt8(bindValue, parameterMeta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof Integer) {
            int num = (Integer) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, MySQLNumberUtils.UNSIGNED_MAX_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof Short) {
            int num = (Short) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, MySQLNumberUtils.UNSIGNED_MAX_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof Byte) {
            int num = (Byte) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, MySQLNumberUtils.UNSIGNED_MAX_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof String) {
            try {
                BigInteger big = new BigInteger((String) nonNullValue);
                int8 = bigIntegerToInt8(bindValue, parameterMeta.mysqlType, big);
            } catch (NumberFormatException e) {
                throw BindUtils.createTypeNotMatchException(bindValue);
            }
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw BindUtils.createNotSupportFractionException(bindValue);
            } else {
                int8 = bigIntegerToInt8(bindValue, parameterMeta.mysqlType, num.toBigInteger());
            }
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }

        PacketUtils.writeInt8(buffer, int8);
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToDouble(final ByteBuf buffer, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final double value;
        if (nonNullValue instanceof Double) {
            value = (Double) nonNullValue;
        } else if (nonNullValue instanceof Float) {
            value = (Float) nonNullValue;
        } else if (nonNullValue instanceof String) {
            try {
                value = Double.parseDouble((String) nonNullValue);
            } catch (NumberFormatException e) {
                throw BindUtils.createTypeNotMatchException(bindValue);
            }
        } else if (nonNullValue instanceof Integer) {
            value = ((Integer) nonNullValue).doubleValue();
        } else if (nonNullValue instanceof Short) {
            value = ((Short) nonNullValue).doubleValue();
        } else if (nonNullValue instanceof Byte) {
            value = ((Byte) nonNullValue).doubleValue();
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }

        PacketUtils.writeInt8(buffer, Double.doubleToLongBits(value));
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToTime(final ByteBuf buffer, final MySQLColumnMeta parameterMeta, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();

        final int microPrecision = MySQLColumnMeta.obtainDateTimeTypePrecision(parameterMeta);
        final int length = microPrecision > 0 ? 12 : 8;

        if (nonNullValue instanceof Duration) {
            Duration duration = (Duration) nonNullValue;
            buffer.writeByte(length); //1. length

            buffer.writeByte(duration.isNegative() ? 1 : 0); //2. is_negative
            duration = duration.abs();
            if (duration.compareTo(Constants.MAX_DURATION) > 0) {
                throw new BindParameterException(String.format(
                        "Bind parameter[%s] MySQLType[%s] Duration[%s] beyond [-838:59:59,838:59:59]"
                        , bindValue.getParamIndex(), bindValue.getType(), duration), bindValue.getParamIndex());
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
                throw BindUtils.createTypeNotMatchException(bindValue, e);
            }
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
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
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToDate(final ByteBuf buffer, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();

        final LocalDate date;
        if (nonNullValue instanceof LocalDate) {
            date = (LocalDate) nonNullValue;

        } else if (nonNullValue instanceof String) {
            try {
                date = LocalDate.parse((String) nonNullValue);
            } catch (DateTimeParseException e) {
                throw BindUtils.createTypeNotMatchException(bindValue, e);
            }
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }
        buffer.writeByte(4); // length
        PacketUtils.writeInt2(buffer, date.getYear()); // year
        buffer.writeByte(date.getMonthValue()); // month
        buffer.writeByte(date.getDayOfMonth()); // day
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToDatetime(final ByteBuf buffer, final MySQLColumnMeta parameterMeta, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();

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
                throw BindUtils.createTypeNotMatchException(bindValue, e);
            }
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }

        final int microPrecision = MySQLColumnMeta.obtainDateTimeTypePrecision(parameterMeta);
        final int length = microPrecision > 0 ? 11 : 7;
        buffer.writeByte(length); // length

        PacketUtils.writeInt2(buffer, dateTime.getYear()); // year
        buffer.writeByte(dateTime.getMonthValue()); // month
        buffer.writeByte(dateTime.getDayOfMonth()); // day

        buffer.writeByte(dateTime.getHour()); // hour
        buffer.writeByte(dateTime.getMinute()); // minute
        buffer.writeByte(dateTime.getSecond()); // second

        if (length == 11) {
            // micro second
            PacketUtils.writeInt4(buffer
                    , truncateMicroSeconds(dateTime.get(ChronoField.MICRO_OF_SECOND), microPrecision));
        }

    }


    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToBit(final ByteBuf buffer, final MySQLColumnMeta parameterMeta, final BindValue bindValue) {
        final String bits = BindUtils.bindToBits(bindValue, buffer);
        if (bits.length() < 4 || bits.length() > (parameterMeta.length + 3)) {
            throw BindUtils.createNumberRangErrorException(bindValue, 1, parameterMeta.length);
        }
        PacketUtils.writeStringLenEnc(buffer, bits.getBytes(this.adjutant.obtainCharsetClient()));
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToStringType(final ByteBuf buffer, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final Charset charset = this.adjutant.obtainCharsetClient();
        if (nonNullValue instanceof CharSequence || nonNullValue instanceof Character) {
            PacketUtils.writeStringLenEnc(buffer, nonNullValue.toString().getBytes(charset));
        } else if (nonNullValue instanceof byte[]) {
            PacketUtils.writeStringLenEnc(buffer, (byte[]) nonNullValue);
        } else if (nonNullValue instanceof Enum) {
            PacketUtils.writeStringLenEnc(buffer, ((Enum<?>) nonNullValue).name().getBytes(charset));
        } else if (nonNullValue instanceof Geometry) {
            // TODO add code
            throw BindUtils.createTypeNotMatchException(bindValue);
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }

    }


    /*################################## blow private static method ##################################*/

    /**
     * @see #bindToTime(ByteBuf, MySQLColumnMeta, BindValue)
     * @see #bindToDatetime(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private static int truncateMicroSeconds(final int microSeconds, final int precision) {
        final int newMicroSeconds;
        switch (precision) {
            case 0:
                newMicroSeconds = 0;
                break;
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6: {
                int micro = microSeconds % 100_0000;
                int unit = 1;
                final int num = 6 - precision;
                for (int i = 0; i < num; i++) {
                    unit *= 10;
                }
                if (unit > 0) {
                    micro -= (micro % unit);
                }
                newMicroSeconds = micro;
            }
            break;
            default:
                throw new IllegalArgumentException(String.format("precision[%s] not in [0,6]", precision));
        }
        return newMicroSeconds;
    }


    /*################################## blow private static convert method ##################################*/

    /**
     * @see #bindToInt4(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private static int longToInt4(BindValue bindValue, MySQLType mySQLType, final long num) {
        if (mySQLType.isUnsigned()) {
            long unsignedMaxInt = Integer.toUnsignedLong(-1);
            if (num < 0 || num > unsignedMaxInt) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxInt);
            }
        } else if (num < Integer.MIN_VALUE || num > Integer.MAX_VALUE) {
            throw BindUtils.createNumberRangErrorException(bindValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        return (int) num;
    }

    /**
     * @see #bindToInt4(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private static int bigIntegerToIn4(BindValue bindValue, MySQLType mySQLType, final BigInteger num) {
        if (mySQLType.isUnsigned()) {
            BigInteger unsignedMaxInt = BigInteger.valueOf(Integer.toUnsignedLong(-1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxInt) > 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxInt);
            }
        } else if (num.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
            throw BindUtils.createNumberRangErrorException(bindValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        return num.intValue();
    }

    /**
     * @see #bindToInt8(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private static long bigIntegerToInt8(BindValue bindValue, MySQLType mySQLType, final BigInteger num) {
        if (mySQLType.isUnsigned()) {
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(MySQLNumberUtils.UNSIGNED_MAX_LONG) > 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, MySQLNumberUtils.UNSIGNED_MAX_LONG);
            }
        } else if (num.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            throw BindUtils.createNumberRangErrorException(bindValue, Long.MIN_VALUE, Long.MAX_VALUE);
        }
        return num.longValue();
    }

    private static int longTotInt1(BindValue bindValue, MySQLType mySQLType, final long num) {

        if (mySQLType.isUnsigned()) {
            int unsignedMaxByte = Byte.toUnsignedInt((byte) -1);
            if (num < 0 || num > unsignedMaxByte) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxByte);
            }
        } else if (num < Byte.MIN_VALUE || num > Byte.MAX_VALUE) {
            throw BindUtils.createNumberRangErrorException(bindValue, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        return (int) num;
    }

    private static int longTotInt2(BindValue bindValue, MySQLType mySQLType, final long num) {

        if (mySQLType.isUnsigned()) {
            int unsignedMaxShort = Short.toUnsignedInt((short) -1);
            if (num < 0 || num > unsignedMaxShort) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxShort);
            }
        } else if (num < Short.MIN_VALUE || num > Short.MAX_VALUE) {
            throw BindUtils.createNumberRangErrorException(bindValue, Short.MIN_VALUE, Short.MAX_VALUE);
        }
        return (int) num;
    }

    private static int bigIntegerTotInt1(BindValue bindValue, MySQLType mySQLType, final BigInteger num) {

        if (mySQLType.isUnsigned()) {
            BigInteger unsignedMaxByte = BigInteger.valueOf(Byte.toUnsignedInt((byte) -1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxByte) > 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxByte);
            }
        } else if (num.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) > 0) {
            throw BindUtils.createNumberRangErrorException(bindValue, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        return num.intValue();
    }

    private static int bigIntegerTotInt2(BindValue bindValue, MySQLType mySQLType, final BigInteger num) {

        if (mySQLType.isUnsigned()) {
            BigInteger unsignedMaxInt2 = BigInteger.valueOf(Short.toUnsignedInt((byte) -1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxInt2) > 0) {
                throw BindUtils.createNumberRangErrorException(bindValue, 0, unsignedMaxInt2);
            }
        } else if (num.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) > 0) {
            throw BindUtils.createNumberRangErrorException(bindValue, Short.MIN_VALUE, Short.MAX_VALUE);
        }
        return num.intValue();
    }


    interface LongParameterWriter {

        Flux<ByteBuf> write(List<BindValue> valueList);
    }


}
