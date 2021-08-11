package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.vendor.result.ResultRowSink;
import io.jdbd.vendor.result.ResultSetReader;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.util.GeometryUtils;
import io.jdbd.vendor.util.JdbdBufferUtils;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

final class DefaultResultSetReader implements ResultSetReader {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultResultSetReader.class);

    private static final byte[] ZERO_BYTE = new byte[0];


    private final StmtTask stmtTask;

    final TaskAdjutant adjutant;

    private final ResultRowSink sink;

    private final Charset clientCharset;

    PgRowMeta rowMeta;

    private Phase phase = Phase.READ_ROW_META;

    private DefaultResultSetReader(StmtTask stmtTask, ResultRowSink sink) {
        this.stmtTask = stmtTask;
        this.adjutant = stmtTask.adjutant();
        this.sink = sink;
        this.clientCharset = this.adjutant.clientCharset();
    }

    @Override
    public final boolean read(final ByteBuf cumulateBuffer, final Consumer<Object> statesConsumer) {
        boolean resultSetEnd = false, continueRead = true;
        while (continueRead) {
            switch (this.phase) {
                case READ_ROW_META: {
                    this.rowMeta = PgRowMeta.read(cumulateBuffer, this.adjutant);
                    if (this.rowMeta.getColumnCount() == 0) {
                        this.phase = Phase.READ_RESULT_TERMINATOR;
                    } else {
                        this.phase = Phase.READ_ROWS;
                    }
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
                    continueRead = false;
                    resultSetEnd = true;
                }
                break;
                case END:
                    throw new IllegalStateException(String.format("%s can't reuse.", this));
                default:
                    throw PgExceptions.createUnknownEnumException(this.phase);
            }
        }

        return resultSetEnd;
    }

    @Override
    public final boolean isResettable() {
        return false;
    }


    /**
     * @return true : read row data end.
     * @see #read(ByteBuf, Consumer)
     */
    private boolean readRowData(final ByteBuf cumulateBuffer) {
        final PgRowMeta rowMeta = Objects.requireNonNull(this.rowMeta, "this.rowMeta");
        final PgColumnMeta[] columnMetaArray = rowMeta.columnMetaArray;
        final ResultRowSink sink = this.sink;
        final boolean isCanceled = sink.isCancelled();

        Object[] columnValueArray;
        int bodyIndex, nextRowIndex;
        while (Messages.hasOneMessage(cumulateBuffer)) {
            if (cumulateBuffer.getByte(cumulateBuffer.readerIndex()) != Messages.D) {
                return true;
            }
            cumulateBuffer.readByte(); // skip message type byte
            bodyIndex = cumulateBuffer.readerIndex();
            nextRowIndex = bodyIndex + cumulateBuffer.readInt();

            if (cumulateBuffer.readShort() != columnMetaArray.length) {
                String m = String.format("Server RowData message column count[%s] and RowDescription[%s] not match."
                        , cumulateBuffer.getShort(cumulateBuffer.readerIndex() - 2), columnMetaArray.length);
                throw new PgJdbdException(m);
            }

            if (isCanceled) {
                cumulateBuffer.readerIndex(nextRowIndex);// skip row
                continue;
            }
            columnValueArray = new Object[columnMetaArray.length];
            PgColumnMeta meta;
            byte[] bytesValue;
            for (int i = 0, valueLength; i < columnMetaArray.length; i++) {
                valueLength = cumulateBuffer.readInt();
                if (valueLength == -1) {
                    // -1 indicates a NULL column value.
                    continue;
                }

                if (valueLength == 0) {
                    bytesValue = ZERO_BYTE;
                } else {
                    bytesValue = new byte[valueLength];
                    cumulateBuffer.readBytes(bytesValue);
                }
                meta = columnMetaArray[i];
                if (meta.textFormat) {
                    columnValueArray[i] = parseColumnFromText(bytesValue, meta);
                } else {
                    columnValueArray[i] = parseColumnFromBinary(bytesValue, meta);
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
    private Object parseColumnFromText(final byte[] bytesValue, final PgColumnMeta meta) {
        final String textValue = new String(bytesValue, this.clientCharset);
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
                value = OffsetTime.parse(textValue, PgTimes.ISO_OFFSET_TIME__FORMATTER);
            }
            break;
            case PgConstant.TYPE_CHAR:
            case PgConstant.TYPE_VARCHAR:
            case PgConstant.TYPE_JSON:
            case PgConstant.TYPE_JSONB:
            case PgConstant.TYPE_MONEY:// money format dependent on locale,so can't(also don't need) convert.
            case PgConstant.TYPE_NAME:
            case PgConstant.TYPE_XML: {
                value = textValue;
            }
            break;
            case PgConstant.TYPE_BYTEA: {
                byte[] bytes;
                if (textValue.startsWith("\\x")) {
                    bytes = textValue.substring(2).getBytes(StandardCharsets.UTF_8);
                    bytes = JdbdBufferUtils.decodeHex(bytes, bytes.length);
                } else {
                    bytes = bytesValue;
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
                value = Duration.parse(textValue);
            }
            break;
            case PgConstant.TYPE_POINT: {
                value = LongBinaries.fromArray(GeometryUtils.pointValueToWkb(textValue, false));
            }
            break;
            // case PgConstant.TYPE_LINE: //Values of type line are output in the following form : { A, B, C } ,so can't convert to WKB,not support now.
            case PgConstant.TYPE_LSEG: {
                value = "";
            }
            break;
            case PgConstant.TYPE_PATH:
            case PgConstant.TYPE_POLYGON:
            case PgConstant.TYPE_CIRCLE: {
                throw new PgJdbdException("Not Support");
            }
//            case PgConstant.TYPE_INT4: {
//
//            }
//            break;
//            case PgConstant.TYPE_INT4: {
//
//            }
//            break;
//            case PgConstant.TYPE_INT4: {
//
//            }
//            break;
//            case PgConstant.TYPE_INT4: {
//
//            }
//            break;
//            case PgConstant.TYPE_INT4: {
//
//            }
//            break;
//            case PgConstant.TYPE_INT4: {
//
//            }
//            break;
//            case PgConstant.TYPE_INT4: {
//
//            }
//            break;
            default: {
                // unknown type
                LOG.debug("Unknown postgre data type, Meta[{}].", meta);
                value = textValue;
            }
        }
        return value;
    }

    private Object parseColumnFromBinary(final byte[] valueBytes, final PgColumnMeta meta) {

        return null;
    }

    /**
     * @see #parseColumnFromText(byte[], PgColumnMeta)
     */
    private BitSet parseBitSetFromText(final String textValue, final PgColumnMeta meta) {
        final int length = textValue.length();
        final byte[] bytes = new byte[(length + 7) >> 3];
        char ch;
        for (int i = 0; i < length; i++) {
            ch = textValue.charAt(i);
            if (ch == '1') {
                bytes[i >> 3] |= (1 << (i & 7));
            } else if (ch != '0') {
                throw createResponseTextColumnValueError(meta, textValue);
            }
        }
        return BitSet.valueOf(bytes);
    }


    public static JdbdSQLException createResponseTextColumnValueError(PgColumnMeta meta, String textValue) {
        String m = String.format("Server response text value[%s] error for PgColumnMeta[%s].", textValue, meta);
        return new JdbdSQLException(new SQLException(m));
    }


    private static boolean convertToBoolean(String textValue) {
        final boolean value;
        switch (textValue) {
            case "true":
            case "yes":
            case "on":
            case "1":
                value = true;
                break;
            case "false":
            case "no":
            case "off":
            case "0":
                value = false;
            default:
                throw new IllegalArgumentException(String.format("text[%s] couldn't convert to boolean.", textValue));
        }
        return value;
    }


    enum Phase {
        READ_ROW_META,
        READ_ROWS,
        READ_RESULT_TERMINATOR,
        END
    }

}
