package io.jdbd.mysql.protocol.client;

import io.jdbd.BindParameterException;
import io.jdbd.SQLBindParameterException;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.mysql.util.MySQLNumberUtils;
import io.jdbd.type.Geometry;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.time.*;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

final class BinaryStatementCommandWriter implements StatementCommandWriter {

    private final int statementId;

    private final MySQLColumnMeta[] paramMetaArray;

    private final boolean query;

    private final Supplier<Integer> sequenceIdSupplier;

    private final ClientProtocolAdjutant adjutant;

    private final Properties properties;

    private final LongParameterWriter longParameterWriter;

    BinaryStatementCommandWriter(final int statementId, MySQLColumnMeta[] paramMetaArray, boolean query
            , Supplier<Integer> sequenceIdSupplier, ClientProtocolAdjutant adjutant) {
        this.statementId = statementId;
        this.paramMetaArray = paramMetaArray;
        this.query = query;
        this.sequenceIdSupplier = sequenceIdSupplier;

        this.adjutant = adjutant;
        this.properties = adjutant.obtainHostInfo().getProperties();
        this.longParameterWriter = new BinaryLongParameterWriter(statementId, adjutant, sequenceIdSupplier);
    }

    @Override
    public Publisher<ByteBuf> writeCommand(final List<BindValue> parameterGroup) {
        final int size = parameterGroup.size();
        final MySQLColumnMeta[] paramMetaArray = this.paramMetaArray;
        if (size != paramMetaArray.length) {
            return Mono.error(new SQLBindParameterException(
                    String.format("Bind parameter size[%s] and sql parameter size[%s] not match."
                            , size, this.paramMetaArray.length)));
        }
        List<BindValue> longParamList = null;

        for (int i = 0; i < size; i++) {
            BindValue bindValue = parameterGroup.get(i);
            if (bindValue.getParamIndex() != i) {
                IllegalStateException e = new IllegalStateException(
                        String.format("BindValue param index[%s] and position[%s] not match.", bindValue.getParamIndex(), i));
                return Flux.error(e);
            } else if (bindValue.getType() != paramMetaArray[i].mysqlType) {
                IllegalStateException e = new IllegalStateException(
                        String.format("BindValue param index[%s] SQLType[%s] and parameter type[%s] not match."
                                , bindValue.getParamIndex(), bindValue.getType(), paramMetaArray[i].mysqlType));
                return Flux.error(e);
            } else if (bindValue.isLongData()) {
                if (longParamList == null) {
                    longParamList = new ArrayList<>();
                }
                longParamList.add(bindValue);
            }

        }

        final Publisher<ByteBuf> publisher;
        if (size == 0) {
            publisher = Flux.create(sink -> {
                ByteBuf packet = createExecutePacketBuffer(10);
                PacketUtils.publishBigPacket(packet, sink, this.sequenceIdSupplier
                        , this.adjutant::createPayloadBuffer, true);
            });
        } else if (longParamList == null) {
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
            for (int length; i < parameterCount; i++) {
                BindValue bindValue = parameterGroup.get(i);
                if (bindValue.getValue() == null) {
                    nullBitsMap[i / 8] |= (1 << (i & 7));
                } else if (!bindValue.isLongData()) {
                    length = obtainParameterValueLength(parameterMetaArray[i], bindValue);
                    parameterValueLength += length;
                }
            }
            i = 0;
        } catch (Exception e) {
            sink.error(new BindParameterException(String.format("Parameter[%s] type not compatibility", i), i));
            return;
        }

        final int prefixLength = 10 + nullBitsMap.length + 1 + (parameterCount << 1);
        final ByteBuf packetBuffer;
        if (parameterValueLength < (PacketUtils.MAX_PAYLOAD - prefixLength)) {
            packetBuffer = createExecutePacketBuffer(prefixLength + (int) parameterValueLength);
        } else {
            packetBuffer = createExecutePacketBuffer(PacketUtils.MAX_PAYLOAD);
        }
        packetBuffer.writeBytes(nullBitsMap); //fill null_bitmap
        packetBuffer.writeByte(1); //fill new_params_bind_flag
        //fill  parameter_types
        for (BindValue value : parameterGroup) {
            PacketUtils.writeInt2(packetBuffer, value.getType().parameterType);
        }
        //fill parameter_values
        ByteBuf buffer = packetBuffer;
        try {
            long restPayloadLength = prefixLength + parameterValueLength;

            for (i = 0; i < parameterCount; i++) {
                BindValue bindValue = parameterGroup.get(i);
                if (bindValue.isLongData() || bindValue.getValue() == null) {
                    continue;
                }
                // bind parameter bto packet buffer
                bindParameter(buffer, parameterMetaArray[i], bindValue);

                if ((buffer == packetBuffer && buffer.readableBytes() >= PacketUtils.MAX_PACKET)
                        || buffer.readableBytes() >= PacketUtils.MAX_PAYLOAD) {
                    if (buffer == packetBuffer) {
                        restPayloadLength -= PacketUtils.publishBigPacket(buffer, sink, this.sequenceIdSupplier
                                , this.adjutant::createPayloadBuffer, false);
                    } else {
                        restPayloadLength -= PacketUtils.publishBigPayload(buffer, sink, this.sequenceIdSupplier
                                , this.adjutant::createPayloadBuffer, false);
                    }
                    final ByteBuf tempBuffer;
                    if (restPayloadLength < 0L) {
                        // this 'if' block handle restPayloadLength error, eg: GEOMETRY,UNKNOWN
                        tempBuffer = this.adjutant.createPacketBuffer(buffer.readableBytes());
                    } else if (restPayloadLength < PacketUtils.MAX_PAYLOAD) {
                        tempBuffer = this.adjutant.createPayloadBuffer((int) restPayloadLength);
                    } else {
                        tempBuffer = this.adjutant.createPayloadBuffer(PacketUtils.MAX_PAYLOAD);
                    }

                    tempBuffer.writeBytes(buffer);
                    buffer.release();
                    buffer = tempBuffer;
                }

            }
            if (buffer == packetBuffer) {
                PacketUtils.publishBigPacket(buffer, sink, this.sequenceIdSupplier
                        , this.adjutant::createPayloadBuffer, true);
            } else {
                PacketUtils.publishBigPayload(buffer, sink, this.sequenceIdSupplier
                        , this.adjutant::createPayloadBuffer, true);
            }
        } catch (Throwable e) {
            buffer.release();
            sink.error(new BindParameterException(String.format("Bind parameter[%s] write error.", i), e, i));
        }
    }

    /**
     * @see #createExecutionPacketPublisher(List)
     */
    private ByteBuf createExecutePacketBuffer(int initialPayloadCapacity) {
        ByteBuf packetBuffer = this.adjutant.createPacketBuffer(initialPayloadCapacity);

        packetBuffer.writeByte(PacketUtils.COM_STMT_EXECUTE); // 1.status
        PacketUtils.writeInt4(packetBuffer, this.statementId);// 2. statement_id
        //3.cursor Flags, reactive api not support cursor
        if (this.query && this.properties.getOrDefault(PropertyKey.useCursorFetch, Boolean.class)) {
            // we only create cursor-backed result sets if
            // a) The query is a SELECT
            // b) The server supports it
            // c) We know it is forward-only (note this doesn't preclude updatable result sets)
            //TODO d) The user has set a fetch size
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
    private int obtainParameterValueLength(MySQLColumnMeta parameterMeta, BindValue bindValue) {
        final Object value = bindValue.getRequiredValue();
        int length;
        switch (bindValue.getType()) {
            case INT:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                length = 4;
                break;
            case DATE:
                length = 5;
                break;
            case BIGINT:
            case INT_UNSIGNED:
            case BIGINT_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case BIT:
                length = 8;
                break;
            case BOOLEAN:
            case TINYINT:
            case TINYINT_UNSIGNED:
                length = 1;
                break;
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case YEAR:
                length = 2;
                break;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                length = (int) parameterMeta.length;
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
                if (value instanceof String) {
                    length = ((String) value).length() * this.adjutant.obtainMaxBytesPerCharClient();
                } else if (value instanceof byte[]) {
                    length = ((byte[]) value).length;
                } else if (value instanceof ByteArrayInputStream) {
                    length = ((ByteArrayInputStream) value).available();
                } else if (value instanceof ByteBuffer) {
                    length = ((ByteBuffer) value).remaining();
                } else if (value instanceof CharBuffer) {
                    length = ((CharBuffer) value).remaining();
                } else {
                    String m = String.format("Bind parameter[%s] not support type[%s]"
                            , bindValue.getParamIndex(), value.getClass().getName());
                    throw new BindParameterException(m, bindValue.getParamIndex());
                }
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
            case DATETIME:
            case TIMESTAMP:
                bindToDatetime(buffer, parameterMeta, bindValue);
                break;
            case UNKNOWN:
            case GEOMETRY:
                //TODO add code
                throw createTypeNotMatchMessage(bindValue);
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
                throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxByte);
            }
            int1 = num;
        } else if (nonNullValue instanceof Boolean) {
            int1 = (Boolean) nonNullValue ? 1 : 0;
        } else if (nonNullValue instanceof Integer) {
            int1 = longTotInt1(bindValue, parameterMeta, (Integer) nonNullValue);
        } else if (nonNullValue instanceof String) {
            try {
                int1 = longTotInt1(bindValue, parameterMeta, Integer.parseInt((String) nonNullValue));
            } catch (NumberFormatException e) {
                throw createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof Short) {
            int1 = longTotInt1(bindValue, parameterMeta, (Short) nonNullValue);
        } else if (nonNullValue instanceof Long) {
            int1 = longTotInt1(bindValue, parameterMeta, (Long) nonNullValue);
        } else if (nonNullValue instanceof BigInteger) {
            int1 = bigIntegerTotInt1(bindValue, parameterMeta, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw createTypeNotMatchMessage(bindValue);
            } else {
                int1 = bigIntegerTotInt1(bindValue, parameterMeta, num.toBigInteger());
            }
        } else {
            throw createTypeNotMatchMessage(bindValue);
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
            if (parameterMeta.isUnsigned()) {
                if (num < 0 || num > unsignedMaxShort) {
                    throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxShort);
                }
            }
            int2 = num;
        } else if (nonNullValue instanceof Integer
                || nonNullValue instanceof Byte
                || nonNullValue instanceof Long) {
            int2 = longTotInt2(bindValue, parameterMeta, ((Number) nonNullValue).longValue());
        } else if (nonNullValue instanceof String) {
            try {
                int2 = longTotInt2(bindValue, parameterMeta, Long.parseLong((String) nonNullValue));
            } catch (NumberFormatException e) {
                throw createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof BigInteger) {
            int2 = bigIntegerTotInt2(bindValue, parameterMeta, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw createTypeNotMatchMessage(bindValue);
            } else {
                int2 = bigIntegerTotInt2(bindValue, parameterMeta, num.toBigInteger());
            }
        } else {
            throw createTypeNotMatchMessage(bindValue);
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
            throw createTypeNotMatchMessage(bindValue);
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
            if (parameterMeta.isUnsigned()) {
                throw createNotSupportTypeWithUnsigned(bindValue);
            }
            int4 = (Integer) nonNullValue;
        } else if (nonNullValue instanceof Long) {
            int4 = longToInt4(bindValue, parameterMeta, (Long) nonNullValue);
        } else if (nonNullValue instanceof String) {
            int num;
            try {
                num = longToInt4(bindValue, parameterMeta, Long.parseLong((String) nonNullValue));
            } catch (NumberFormatException e) {
                try {
                    num = bigIntegerToIn4(bindValue, parameterMeta, new BigInteger((String) nonNullValue));
                } catch (NumberFormatException ne) {
                    throw createTypeNotMatchMessage(bindValue);
                }
            }
            int4 = num;
        } else if (nonNullValue instanceof BigInteger) {
            int4 = bigIntegerToIn4(bindValue, parameterMeta, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof Short) {
            if (parameterMeta.isUnsigned()) {
                throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxInt);
            }
            int4 = ((Short) nonNullValue).intValue();
        } else if (nonNullValue instanceof Byte) {
            if (parameterMeta.isUnsigned()) {
                throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxInt);
            }
            int4 = ((Byte) nonNullValue).intValue();
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw createTypeNotMatchMessage(bindValue);
            } else if (parameterMeta.isUnsigned()) {
                if (num.compareTo(BigDecimal.ZERO) < 0
                        || num.compareTo(BigDecimal.valueOf(unsignedMaxInt)) > 0) {
                    throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxInt);
                }
            } else if (num.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0
                    || num.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
                throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxInt);
            }
            int4 = num.intValue();
        } else {
            throw createTypeNotMatchMessage(bindValue);
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
                throw createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof Short) {
            floatValue = ((Short) nonNullValue).floatValue();
        } else if (nonNullValue instanceof Byte) {
            floatValue = ((Byte) nonNullValue).floatValue();
        } else {
            throw createTypeNotMatchMessage(bindValue);
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
                throw createNumberRangErrorMessage(bindValue, 0, MySQLNumberUtils.UNSIGNED_MAX_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof BigInteger) {
            int8 = bigIntegerToInt8(bindValue, parameterMeta, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof Integer) {
            int num = (Integer) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw createNumberRangErrorMessage(bindValue, 0, MySQLNumberUtils.UNSIGNED_MAX_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof Short) {
            int num = (Short) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw createNumberRangErrorMessage(bindValue, 0, MySQLNumberUtils.UNSIGNED_MAX_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof Byte) {
            int num = (Byte) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw createNumberRangErrorMessage(bindValue, 0, MySQLNumberUtils.UNSIGNED_MAX_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof String) {
            try {
                BigInteger big = new BigInteger((String) nonNullValue);
                int8 = bigIntegerToInt8(bindValue, parameterMeta, big);
            } catch (NumberFormatException e) {
                throw createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw createTypeNotMatchMessage(bindValue);
            } else {
                int8 = bigIntegerToInt8(bindValue, parameterMeta, num.toBigInteger());
            }
        } else {
            throw createTypeNotMatchMessage(bindValue);
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
                throw createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof Integer) {
            value = ((Integer) nonNullValue).doubleValue();
        } else if (nonNullValue instanceof Short) {
            value = ((Short) nonNullValue).doubleValue();
        } else if (nonNullValue instanceof Byte) {
            value = ((Byte) nonNullValue).doubleValue();
        } else {
            throw createTypeNotMatchMessage(bindValue);
        }

        PacketUtils.writeInt8(buffer, Double.doubleToLongBits(value));
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToTime(final ByteBuf buffer, final MySQLColumnMeta parameterMeta, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();

        final int length;
        if (parameterMeta.decimals > 0 && parameterMeta.decimals < 7) {
            length = 12;
        } else {
            int decimal = (int) (parameterMeta.length - 11L);
            if (decimal == 0) {
                length = 8;
            } else if (decimal < 7) {
                length = 12;
            } else {
                throw new BindParameterException(String.format("Parameter[%s] meta length[%s] error,"
                        , bindValue.getParamIndex(), parameterMeta.length), bindValue.getParamIndex());
            }
        }

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
                PacketUtils.writeInt4(buffer, (int) duration.toMillis());//7, micro seconds
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
        } else {
            throw createTypeNotMatchMessage(bindValue);
        }
        if (time != null) {
            buffer.writeByte(length); //1. length
            buffer.writeByte(0); //2. is_negative
            buffer.writeZero(4); //3. days

            buffer.writeByte(time.getHour()); //4. hour
            buffer.writeByte(time.getMinute()); //5. minute
            buffer.writeByte(time.getSecond()); ///6. second

            if (length == 12) {
                PacketUtils.writeInt4(buffer, time.get(ChronoField.MICRO_OF_SECOND));//7, micro seconds
            }
        }

    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToDatetime(final ByteBuf buffer, final MySQLColumnMeta parameterMeta, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();

        if (nonNullValue instanceof LocalDate) {
            LocalDate date = (LocalDate) nonNullValue;

            buffer.writeByte(4); // length
            PacketUtils.writeInt2(buffer, date.getYear()); // year
            buffer.writeByte(date.getMonthValue()); // month
            buffer.writeByte(date.getDayOfMonth()); // day
            return;
        }
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
        } else {
            throw createTypeNotMatchMessage(bindValue);
        }

        final int length;
        if (parameterMeta.decimals > 0 && parameterMeta.decimals < 7) {
            length = 11;
        } else {
            int decimal = (int) (parameterMeta.length - 20L);
            if (decimal == 0) {
                length = 7;
            } else if (decimal < 7) {
                length = 11;
            } else {
                throw new BindParameterException(String.format("Parameter[%s] meta length[%s] error,"
                        , bindValue.getParamIndex(), parameterMeta.length), bindValue.getParamIndex());
            }
        }
        buffer.writeByte(length); // length

        PacketUtils.writeInt2(buffer, dateTime.getYear()); // year
        buffer.writeByte(dateTime.getMonthValue()); // month
        buffer.writeByte(dateTime.getDayOfMonth()); // day

        buffer.writeByte(dateTime.getHour()); // hour
        buffer.writeByte(dateTime.getMinute()); // minute
        buffer.writeByte(dateTime.getSecond()); // second

        PacketUtils.writeInt4(buffer, dateTime.get(ChronoField.MICRO_OF_SECOND));// micro second
    }


    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue)
     */
    private void bindToBit(final ByteBuf buffer, final MySQLColumnMeta parameterMeta, final BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final String bits;
        if (nonNullValue instanceof Long) {
            bits = Long.toBinaryString((Long) nonNullValue);
        } else if (nonNullValue instanceof Integer) {
            bits = Integer.toBinaryString((Integer) nonNullValue);
        } else if (nonNullValue instanceof Short) {
            bits = Integer.toBinaryString((Short) nonNullValue);
        } else if (nonNullValue instanceof Byte) {
            bits = Integer.toBinaryString((Byte) nonNullValue);
        } else if (nonNullValue instanceof String) {
            bits = (String) nonNullValue;
        } else if (nonNullValue instanceof BigInteger) {
            BigInteger big = (BigInteger) nonNullValue;
            if (big.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0
                    || big.compareTo(MySQLNumberUtils.UNSIGNED_MAX_LONG) > 0) {
                throw createNumberRangErrorMessage(bindValue, 1, parameterMeta.length);
            }
            bits = big.toString(2);
        } else {
            throw createTypeNotMatchMessage(bindValue);
        }
        if (bits.length() < 1 || bits.length() > parameterMeta.length) {
            throw createNumberRangErrorMessage(bindValue, 1, parameterMeta.length);
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
            throw createTypeNotMatchMessage(bindValue);
        } else {
            throw createTypeNotMatchMessage(bindValue);
        }

    }


    /*################################## blow private static convert method ##################################*/

    private static int longToInt4(BindValue bindValue, MySQLColumnMeta parameterMeta, final long num) {
        if (parameterMeta.isUnsigned()) {
            long unsignedMaxInt = Integer.toUnsignedLong(-1);
            if (num < 0 || num > unsignedMaxInt) {
                throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxInt);
            }
        } else if (num < Integer.MIN_VALUE || num > Integer.MAX_VALUE) {
            throw createNumberRangErrorMessage(bindValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        return (int) num;
    }

    private static int bigIntegerToIn4(BindValue bindValue, MySQLColumnMeta parameterMeta, final BigInteger num) {
        if (parameterMeta.isUnsigned()) {
            BigInteger unsignedMaxInt = BigInteger.valueOf(Integer.toUnsignedLong(-1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxInt) > 0) {
                throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxInt);
            }
        } else if (num.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
            throw createNumberRangErrorMessage(bindValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        return num.intValue();
    }

    private static long bigIntegerToInt8(BindValue bindValue, MySQLColumnMeta parameterMeta, final BigInteger num) {
        if (parameterMeta.isUnsigned()) {
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(MySQLNumberUtils.UNSIGNED_MAX_LONG) > 0) {
                throw createNumberRangErrorMessage(bindValue, 0, MySQLNumberUtils.UNSIGNED_MAX_LONG);
            }
        } else if (num.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            throw createNumberRangErrorMessage(bindValue, Long.MIN_VALUE, Long.MAX_VALUE);
        }
        return num.longValue();
    }

    private static int longTotInt1(BindValue bindValue, MySQLColumnMeta parameterMeta, final long num) {

        if (parameterMeta.isUnsigned()) {
            int unsignedMaxByte = Byte.toUnsignedInt((byte) -1);
            if (num < 0 || num > unsignedMaxByte) {
                throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxByte);
            }
        } else if (num < Byte.MIN_VALUE || num > Byte.MAX_VALUE) {
            throw createNumberRangErrorMessage(bindValue, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        return (int) num;
    }

    private static int longTotInt2(BindValue bindValue, MySQLColumnMeta parameterMeta, final long num) {

        if (parameterMeta.isUnsigned()) {
            int unsignedMaxShort = Short.toUnsignedInt((short) -1);
            if (num < 0 || num > unsignedMaxShort) {
                throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxShort);
            }
        } else if (num < Short.MIN_VALUE || num > Short.MAX_VALUE) {
            throw createNumberRangErrorMessage(bindValue, Short.MIN_VALUE, Short.MAX_VALUE);
        }
        return (int) num;
    }

    private static int bigIntegerTotInt1(BindValue bindValue, MySQLColumnMeta parameterMeta, final BigInteger num) {

        if (parameterMeta.isUnsigned()) {
            BigInteger unsignedMaxByte = BigInteger.valueOf(Byte.toUnsignedInt((byte) -1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxByte) > 0) {
                throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxByte);
            }
        } else if (num.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) > 0) {
            throw createNumberRangErrorMessage(bindValue, Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        return num.intValue();
    }

    private static int bigIntegerTotInt2(BindValue bindValue, MySQLColumnMeta parameterMeta, final BigInteger num) {

        if (parameterMeta.isUnsigned()) {
            BigInteger unsignedMaxInt2 = BigInteger.valueOf(Short.toUnsignedInt((byte) -1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxInt2) > 0) {
                throw createNumberRangErrorMessage(bindValue, 0, unsignedMaxInt2);
            }
        } else if (num.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) > 0) {
            throw createNumberRangErrorMessage(bindValue, Short.MIN_VALUE, Short.MAX_VALUE);
        }
        return num.intValue();
    }


    /*################################## blow private static exception method ##################################*/

    private static BindParameterException createNotSupportTypeWithUnsigned(BindValue bindValue) {
        throw new BindParameterException(String.format("Bind parameter[%s] is unsigned,not support %s"
                , bindValue.getParamIndex()
                , bindValue.getRequiredValue().getClass().getName())
                , bindValue.getParamIndex());
    }

    private static BindParameterException createNumberRangErrorMessage(BindValue bindValue, Number lower
            , Number upper) {
        return new BindParameterException(String.format("Bind parameter[%s] MySQLType[%s] beyond rang[%s,%s]."
                , bindValue.getParamIndex(), bindValue.getType(), lower, upper)
                , bindValue.getParamIndex());

    }

    private static BindParameterException createTypeNotMatchMessage(BindValue bindValue) {
        return new BindParameterException(String.format("Bind parameter[%s] MySQLType[%s] and JavaType[%s] not match."
                , bindValue.getParamIndex()
                , bindValue.getType()
                , bindValue.getRequiredValue().getClass().getName())
                , bindValue.getParamIndex());
    }


    interface LongParameterWriter {

        Flux<ByteBuf> write(List<BindValue> valueList);
    }


}
