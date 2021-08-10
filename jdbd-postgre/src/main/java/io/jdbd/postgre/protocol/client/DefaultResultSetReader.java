package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.util.DateStyle;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.vendor.result.ResultRowSink;
import io.jdbd.vendor.result.ResultSetReader;
import io.jdbd.vendor.type.LongBinaries;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

final class DefaultResultSetReader implements ResultSetReader {

    private static final byte[] ZERO_BYTE = new byte[0];


    private final StmtTask stmtTask;

    final TaskAdjutant adjutant;

    private final ResultRowSink sink;

    private final DateStyle dateStyle;

    private final Charset clientCharset;

    PgRowMeta rowMeta;

    private Phase phase = Phase.READ_ROW_META;

    private DefaultResultSetReader(StmtTask stmtTask, ResultRowSink sink) {
        this.stmtTask = stmtTask;
        this.adjutant = stmtTask.adjutant();
        this.sink = sink;
        this.dateStyle = this.adjutant.dateStyle();
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
            byte[] valueBytes;
            for (int i = 0, valueLength; i < columnMetaArray.length; i++) {
                valueLength = cumulateBuffer.readInt();
                if (valueLength == -1) {
                    // -1 indicates a NULL column value.
                    continue;
                }

                if (valueLength == 0) {
                    valueBytes = ZERO_BYTE;
                } else {
                    valueBytes = new byte[valueLength];
                    cumulateBuffer.readBytes(valueBytes);
                }
                meta = columnMetaArray[i];
                if (meta.textFormat) {
                    columnValueArray[i] = parseColumnFromText(valueBytes, meta);
                } else {
                    columnValueArray[i] = parseColumnFromBinary(valueBytes, meta);
                }

            }

            sink.next(PgResultRow.create(rowMeta, columnValueArray, this.adjutant));

            cumulateBuffer.readerIndex(nextRowIndex);//avoid tail filler

        }

        return false;
    }


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
                value = LocalDateTime.parse(textValue, PgTimes.ISO_LOCAL_DATETIME_FORMATTER);
            }
            break;
            case PgConstant.TYPE_DATE: {
                value = LocalDate.parse(textValue, DateTimeFormatter.ISO_LOCAL_DATE);
            }
            break;
            case PgConstant.TYPE_TIME: {
                value = LocalTime.parse(textValue, PgTimes.ISO_LOCAL_TIME_FORMATTER);
            }
            break;
            case PgConstant.TYPE_TIMESTAMPTZ: {
                value = OffsetDateTime.parse(textValue, PgTimes.ISO_OFFSET_DATETIME__FORMATTER);
            }
            break;
            case PgConstant.TYPE_TIMETZ: {
                value = OffsetTime.parse(textValue, PgTimes.ISO_OFFSET_TIME__FORMATTER);
            }
            break;
            case PgConstant.TYPE_CHAR:
            case PgConstant.TYPE_VARCHAR:
            case PgConstant.TYPE_JSON:
            case PgConstant.TYPE_XML: {
                value = textValue;
            }
            break;
            case PgConstant.TYPE_JSONB:
            case PgConstant.TYPE_BYTEA: {
                value = LongBinaries.fromArray(bytesValue);
            }
            break;
            case PgConstant.TYPE_VARBIT:
            case PgConstant.TYPE_BIT: {
                final int length = textValue.length();
                byte[] bytes = new byte[length];
                char ch;
                for (int i = 0; i < length; i++) {
                    ch = textValue.charAt(i);
                    if (ch == '0') {

                    } else if (ch == '1') {

                    } else {
                        throw createResponseTextColumnValueError(meta, textValue);
                    }
                }
            }
            break;
            case PgConstant.TYPE_UUID: {
                value = UUID.fromString(textValue);
            }
            break;
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
//            default: {
//                value = bytesValue;
//            }
        }
        return null;
    }

    private Object parseColumnFromBinary(final byte[] valueBytes, final PgColumnMeta meta) {

        return null;
    }


    /**
     * @see #parseColumnFromText(byte[], PgColumnMeta)
     */
    private LocalDate parseLocalDateFromText(String textValue) {
        final LocalDate date;
        switch (this.dateStyle) {
            case ISO: {
                date = LocalDate.parse(textValue, DateTimeFormatter.ISO_LOCAL_DATE);
            }
            break;
            case SQL:
            case German:
            case Postgres:
                // TODO fill
            default:
                throw PgExceptions.createUnknownEnumException(this.dateStyle);
        }
        return date;
    }


    private LocalDateTime parseLocalDateTimeFromText(String textValue) {
        final LocalDateTime dateTime;
        switch (this.dateStyle) {
            case ISO: {
                dateTime = LocalDateTime.parse(textValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            }
            break;
            case SQL:
            case German:
            case Postgres:
            default:
                throw PgExceptions.createUnknownEnumException(this.dateStyle);
        }
        return dateTime;
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
