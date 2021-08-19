package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.type.PgGeometries;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.vendor.result.ResultSetReader;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.type.LongStrings;
import io.jdbd.vendor.util.JdbdBuffers;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAmount;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

final class DefaultResultSetReader implements ResultSetReader {

    static DefaultResultSetReader create(StmtTask stmtTask, ResultSink sink) {
        return new DefaultResultSetReader(stmtTask, sink);
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultResultSetReader.class);

    // We can't use Long.MAX_VALUE or Long.MIN_VALUE for java.sql.date
    // because this would break the 'normalization contract' of the
    // java.sql.Date API.
    // The follow values are the nearest MAX/MIN values with hour,
    // minute, second, millisecond set to 0 - this is used for
    // -infinity / infinity representation in Java
    private static final long DATE_POSITIVE_INFINITY = 9223372036825200000L;
    private static final long DATE_NEGATIVE_INFINITY = -9223372036832400000L;
    private static final long DATE_POSITIVE_SMALLER_INFINITY = 185543533774800000L;
    private static final long DATE_NEGATIVE_SMALLER_INFINITY = -185543533774800000L;


    private final StmtTask stmtTask;

    private final TaskAdjutant adjutant;

    private final ResultSink sink;

    private PgRowMeta rowMeta;

    private Phase phase = Phase.READ_ROW_META;

    private DefaultResultSetReader(StmtTask stmtTask, ResultSink sink) {
        this.stmtTask = stmtTask;
        this.adjutant = stmtTask.adjutant();
        this.sink = sink;
    }

    @Override
    public final boolean read(final ByteBuf cumulateBuffer, final Consumer<Object> statesConsumer) {
        boolean resultSetEnd = false, continueRead = true;
        while (continueRead) {
            switch (this.phase) {
                case READ_ROW_META: {
                    final PgRowMeta rowMeta;
                    rowMeta = PgRowMeta.read(cumulateBuffer, this.stmtTask);
                    this.rowMeta = rowMeta;
                    LOG.trace("Read ResultSet row meta data : {}", rowMeta);
                    this.phase = Phase.READ_ROWS;
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case READ_ROWS: {
                    if (readRowData(cumulateBuffer)) {
                        this.phase = Phase.READ_RESULT_TERMINATOR;
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }
                }
                break;
                case READ_RESULT_TERMINATOR: {
                    final PgRowMeta rowMeta = Objects.requireNonNull(this.rowMeta, "this.rowMeta");
                    resultSetEnd = this.stmtTask.readResultStateWithReturning(cumulateBuffer, false
                            , rowMeta::getResultIndex);
                    continueRead = false;
                }
                break;
                case END:
                    throw new IllegalStateException(String.format("%s can't reuse.", this));
                default:
                    throw PgExceptions.createUnknownEnumException(this.phase);
            }
        }
        if (resultSetEnd) {
            if (this.stmtTask.hasError()) {
                this.phase = Phase.END;
            } else {
                reset(); // for next result set
            }
        }
        return resultSetEnd;
    }

    @Override
    public final boolean isResettable() {
        return true;
    }

    @Override
    public final String toString() {
        return String.format("Class[%s] phase[%s]", getClass().getSimpleName(), this.phase);
    }

    private void reset() {
        this.rowMeta = null;
        this.phase = Phase.READ_ROW_META;
    }


    /**
     * @return true : read row data end.
     * @see #read(ByteBuf, Consumer)
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">DataRow</a>
     */
    private boolean readRowData(final ByteBuf cumulateBuffer) {

        final PgRowMeta rowMeta = Objects.requireNonNull(this.rowMeta, "this.rowMeta");
        if (LOG.isTraceEnabled()) {
            LOG.trace("Read ResultSet row data for meta :{}", rowMeta);
        }

        final PgColumnMeta[] columnMetaArray = rowMeta.columnMetaArray;
        final ResultSink sink = this.sink;
        // if 'true' maybe user cancel or occur error
        final boolean isCanceled = this.stmtTask.hasError() || sink.isCancelled();
        final Charset clientCharset = this.adjutant.clientCharset();
        Object[] columnValueArray;
        while (Messages.hasOneMessage(cumulateBuffer)) {
            final int msgIndex = cumulateBuffer.readerIndex();
            if (cumulateBuffer.getByte(msgIndex) != Messages.D) {
                return true;
            }
            cumulateBuffer.readByte(); // skip message type byte
            final int nextRowIndex = msgIndex + 1 + cumulateBuffer.readInt();

            if (isCanceled) {
                cumulateBuffer.readerIndex(nextRowIndex);// skip row
                continue;
            }
            if (cumulateBuffer.readShort() != columnMetaArray.length) {
                String m = String.format("Server RowData message column count[%s] and RowDescription[%s] not match."
                        , cumulateBuffer.getShort(cumulateBuffer.readerIndex() - 2), columnMetaArray.length);
                throw new PgJdbdException(m);
            }

            columnValueArray = new Object[columnMetaArray.length];
            PgColumnMeta meta;
            for (int i = 0, valueLength; i < columnMetaArray.length; i++) {
                valueLength = cumulateBuffer.readInt();
                if (valueLength == -1) {
                    // -1 indicates a NULL column value.
                    continue;
                }

                meta = columnMetaArray[i];
                if (meta.textFormat) {
                    byte[] bytes = new byte[valueLength];
                    cumulateBuffer.readBytes(bytes);
                    columnValueArray[i] = parseColumnFromText(new String(bytes, clientCharset), meta);
                } else {
                    columnValueArray[i] = parseColumnFromBinary(cumulateBuffer, valueLength, meta);
                }

            }

            sink.next(PgResultRow.create(rowMeta, columnValueArray, this.adjutant));

            cumulateBuffer.readerIndex(nextRowIndex);//avoid tail filler

        }

        return false;
    }


    /**
     * @see #readRowData(ByteBuf)
     */
    private Object parseColumnFromText(final String textValue, final PgColumnMeta meta) {
        final Object value;
        switch (meta.columnTypeOid) {
            case PgConstant.TYPE_INT2: {
                value = Short.parseShort(textValue);
            }
            break;
            case PgConstant.TYPE_INT4: {
                value = Integer.parseInt(textValue);
            }
            break;
            case PgConstant.TYPE_OID:
            case PgConstant.TYPE_INT8: {
                value = Long.parseLong(textValue);
            }
            break;
            case PgConstant.TYPE_NUMERIC: {
                value = new BigDecimal(textValue);
            }
            break;
            case PgConstant.TYPE_FLOAT4: {
                value = Float.parseFloat(textValue);
            }
            break;
            case PgConstant.TYPE_FLOAT8: {
                value = Double.parseDouble(textValue);
            }
            break;
            case PgConstant.TYPE_BOOLEAN: {
                if (textValue.equals("t")) {
                    value = Boolean.TRUE;
                } else if (textValue.equals("f")) {
                    value = Boolean.FALSE;
                } else {
                    throw createResponseTextColumnValueError(meta, textValue);
                }
            }
            break;
            case PgConstant.TYPE_TIMESTAMP: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                value = LocalDateTime.parse(textValue, PgTimes.ISO_LOCAL_DATETIME_FORMATTER);
            }
            break;
            case PgConstant.TYPE_DATE: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                value = LocalDate.parse(textValue, DateTimeFormatter.ISO_LOCAL_DATE);
            }
            break;
            case PgConstant.TYPE_TIME: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                value = LocalTime.parse(textValue, PgTimes.ISO_LOCAL_TIME_FORMATTER);
            }
            break;
            case PgConstant.TYPE_TIMESTAMPTZ: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                value = OffsetDateTime.parse(textValue, PgTimes.ISO_OFFSET_DATETIME__FORMATTER);
            }
            break;
            case PgConstant.TYPE_TIMETZ: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                value = OffsetTime.parse(textValue, PgTimes.ISO_OFFSET_TIME_FORMATTER);
            }
            break;
            case PgConstant.TYPE_CHAR:
            case PgConstant.TYPE_VARCHAR:
            case PgConstant.TYPE_JSON:
            case PgConstant.TYPE_JSONB:
            case PgConstant.TYPE_MONEY:// money format dependent on locale,so can't(also don't need) convert.
            case PgConstant.TYPE_NAME:
            case PgConstant.TYPE_MAC_ADDR:
            case PgConstant.TYPE_MAC_ADDR8:
            case PgConstant.TYPE_INET:
            case PgConstant.TYPE_CIDR:
            case PgConstant.TYPE_LINE:
            case PgConstant.TYPE_LSEG:
            case PgConstant.TYPE_BOX:
            case PgConstant.TYPE_XML: {
                value = textValue;
            }
            break;
            case PgConstant.TYPE_BYTEA: {
                byte[] bytes;
                if (textValue.startsWith("\\x")) {
                    bytes = textValue.substring(2).getBytes(StandardCharsets.UTF_8);
                    bytes = JdbdBuffers.decodeHex(bytes, bytes.length);
                } else {
                    //TODO validate this
                    bytes = textValue.getBytes(this.adjutant.clientCharset());
                }
                value = LongBinaries.fromArray(bytes);
            }
            break;
            case PgConstant.TYPE_VARBIT:
            case PgConstant.TYPE_BIT: {
                value = parseBitSetFromText(textValue, meta);
            }
            break;
            case PgConstant.TYPE_UUID: {
                value = UUID.fromString(textValue);
            }
            break;
            case PgConstant.TYPE_INTERVAL: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO-8601
                value = parseTemporalAmountFromText(textValue, meta);
            }
            break;
            case PgConstant.TYPE_POINT: {
                value = PgGeometries.point(textValue);
            }
            break;
            case PgConstant.TYPE_POLYGON:
            case PgConstant.TYPE_PATH: {
                value = LongStrings.fromString(textValue);
            }
            break;
            case PgConstant.TYPE_CIRCLE: {
                value = PgGeometries.circle(textValue);
            }
            break;
            default: {
                // unknown type
                LOG.debug("Unknown postgre data type, Meta[{}].", meta);
                value = textValue;
            }
        }
        return value;
    }

    /**
     * @see #readRowData(ByteBuf)
     */
    private Object parseColumnFromBinary(final ByteBuf cumulateBuffer, final int valueLength, final PgColumnMeta meta) {

        final Object value;
        switch (meta.columnTypeOid) {
            case PgConstant.TYPE_INT2: {
                if (valueLength != 2) {
                    throw createResponseBinaryColumnValueError(cumulateBuffer, valueLength, meta);
                }
                value = cumulateBuffer.readShort();
            }
            break;
            case PgConstant.TYPE_INT4: {
                if (valueLength != 4) {
                    throw createResponseBinaryColumnValueError(cumulateBuffer, valueLength, meta);
                }
                value = cumulateBuffer.readInt();
            }
            break;
            case PgConstant.TYPE_OID:
            case PgConstant.TYPE_INT8: {
                if (valueLength != 8) {
                    throw createResponseBinaryColumnValueError(cumulateBuffer, valueLength, meta);
                }
                value = cumulateBuffer.readLong();
            }
            break;
            case PgConstant.TYPE_NUMERIC: {
                value = parseBinaryDecimal(cumulateBuffer, valueLength, meta);
            }
            break;
            case PgConstant.TYPE_FLOAT4: {
                if (valueLength != 4) {
                    throw createResponseBinaryColumnValueError(cumulateBuffer, valueLength, meta);
                }
                value = Float.intBitsToFloat(cumulateBuffer.readInt());
            }
            break;
            case PgConstant.TYPE_FLOAT8: {
                if (valueLength != 8) {
                    throw createResponseBinaryColumnValueError(cumulateBuffer, valueLength, meta);
                }
                value = Double.longBitsToDouble(cumulateBuffer.readLong());
            }
            break;
            case PgConstant.TYPE_BOOLEAN: {
                if (valueLength != 1) {
                    throw createResponseBinaryColumnValueError(cumulateBuffer, valueLength, meta);
                }
                value = cumulateBuffer.readByte() == 1;
            }
            break;
            case PgConstant.TYPE_TIMESTAMP: {
                if (valueLength != 8) {
                    throw createResponseBinaryColumnValueError(cumulateBuffer, valueLength, meta);
                }
                value = new byte[0]; //TODO fix
            }
            break;
            default: {
                byte[] bytes = new byte[valueLength];
                value = cumulateBuffer.readBytes(bytes);
            }

        }
        return value;
    }

    /**
     * @see #parseColumnFromText(String, PgColumnMeta)
     */
    private BitSet parseBitSetFromText(final String textValue, final PgColumnMeta meta) {
        final int length = textValue.length();
        final byte[] bytes = new byte[(length + 7) >> 3];
        char ch;
        for (int bitIndex = 0, charIndex = length - 1; bitIndex < length; bitIndex++, charIndex--) {
            ch = textValue.charAt(charIndex);
            if (ch == '1') {
                bytes[bitIndex >> 3] |= (1 << (bitIndex & 7));
            } else if (ch != '0') {
                throw createResponseTextColumnValueError(meta, textValue);
            }
        }
        return BitSet.valueOf(bytes);
    }

    /**
     * @see #parseColumnFromText(String, PgColumnMeta)
     */
    private TemporalAmount parseTemporalAmountFromText(final String textValue, final PgColumnMeta meta) {
        final TemporalAmount amount;
        switch (this.adjutant.server().intervalStyle()) {
            case iso_8601: {
                amount = PgTimes.parseIsoInterval(textValue);
            }
            break;
            case postgres:
            case sql_standard:
            case postgres_verbose:
            default:
                throw new IllegalArgumentException(String.format("Cannot parse interval,ColumnMata[%s]", meta));
        }
        return amount;
    }

    private BigDecimal parseBinaryDecimal(final ByteBuf cumulateBuffer, final int valueLength, final PgColumnMeta meta) {

        return null;
    }

    /**
     * @see #parseColumnFromBinary(ByteBuf, int, PgColumnMeta)
     */
    private OffsetDateTime parseLocalDateTimeFromBinaryLong(final ByteBuf cumulateBuffer) {
        final long seconds;
        final int nanos;

        final long time = cumulateBuffer.readLong();
        if (time == Long.MAX_VALUE) {
            seconds = DATE_POSITIVE_INFINITY / 1000;
            nanos = 0;
        } else if (time == Long.MIN_VALUE) {
            seconds = DATE_NEGATIVE_INFINITY / 1000;
            nanos = 0;
        } else {
            final int million = 1000_000;
            long secondPart = time / million;
            int nanoPart = (int) (time - secondPart * million);
            if (nanoPart < 0) {
                secondPart--;
                nanoPart += million;
            }
            nanoPart *= 1000;

            seconds = PgTimes.toJavaSeconds(secondPart);
            nanos = nanoPart;
        }
        return OffsetDateTime.of(LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC), ZoneOffset.UTC);
    }

    /**
     * @see #parseColumnFromBinary(ByteBuf, int, PgColumnMeta)
     */
    private OffsetDateTime parseLocalDateTimeFromBinaryDouble(final ByteBuf cumulateBuffer) {
        final long seconds;
        final int nanos;

        final double time = Double.longBitsToDouble(cumulateBuffer.readLong());
        if (time == Double.POSITIVE_INFINITY) {
            seconds = DATE_POSITIVE_INFINITY / 1000;
            nanos = 0;
        } else if (time == Double.NEGATIVE_INFINITY) {
            seconds = DATE_NEGATIVE_INFINITY / 1000;
            nanos = 0;
        } else {
            final int million = 1000_000;
            long secondPart = (long) time;
            int nanoPart = (int) ((time - secondPart) * million);
            if (nanoPart < 0) {
                secondPart--;
                nanoPart += million;
            }
            nanoPart *= 1000;

            seconds = PgTimes.toJavaSeconds(secondPart);
            nanos = nanoPart;
        }
        return OffsetDateTime.of(LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC), ZoneOffset.UTC);
    }


    private static JdbdSQLException createResponseTextColumnValueError(PgColumnMeta meta, String textValue) {
        String m = String.format("Server response text value[%s] error for PgColumnMeta[%s].", textValue, meta);
        return new JdbdSQLException(new SQLException(m));
    }

    private static JdbdSQLException createResponseBinaryColumnValueError(final ByteBuf cumulateBuffer
            , final int valueLength, final PgColumnMeta meta) {
        byte[] bytes = new byte[valueLength];
        cumulateBuffer.readBytes(bytes);
        String m = String.format("Server response binary value[%s] error for PgColumnMeta[%s].", bytes, meta);
        return new JdbdSQLException(new SQLException(m));
    }



    enum Phase {
        READ_ROW_META,
        READ_ROWS,
        READ_RESULT_TERMINATOR,
        END
    }

}
