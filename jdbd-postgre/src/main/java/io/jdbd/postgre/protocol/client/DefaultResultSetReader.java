package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.vendor.result.ResultSetReader;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.util.JdbdBuffers;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Objects;
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
    public final boolean read(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer)
            throws JdbdException {
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
                    resultSetEnd = this.stmtTask.readResultStateWithReturning(cumulateBuffer, rowMeta::getResultIndex);
                    continueRead = false;
                }
                break;
                case END:
                    throw new IllegalStateException(String.format("%s can't reuse.", this));
                default:
                    throw PgExceptions.createUnexpectedEnumException(this.phase);
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
        PgColumnMeta meta;
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
            for (int i = 0, valueLength; i < columnMetaArray.length; i++) {
                valueLength = cumulateBuffer.readInt();
                if (valueLength == -1) {
                    // -1 indicates a NULL column value.
                    columnValueArray[i] = null;
                    continue;
                }
                meta = columnMetaArray[i];
                if (meta.textFormat) {
                    columnValueArray[i] = parseTextColumn(cumulateBuffer, valueLength, rowMeta, meta);
                } else {
                    columnValueArray[i] = parseColumnFromBinary(cumulateBuffer, valueLength, meta);
                }
            }
            sink.next(PgResultRow.create(rowMeta, columnValueArray, this.adjutant));
            cumulateBuffer.readerIndex(nextRowIndex);//avoid tail filler
        }

        return false;
    }

    private static Object parseTextColumn(final ByteBuf cumulateBuffer, final int valueLength, final PgRowMeta rowMeta
            , final PgColumnMeta meta) {
        final byte[] bytes = new byte[valueLength];
        cumulateBuffer.readBytes(bytes);
        final Object value;
        if (meta.sqlType == PgType.BYTEA) {
            if (bytes.length > 1 && bytes[0] == '\\' && bytes[1] == 'x') {
                byte[] v = Arrays.copyOfRange(bytes, 2, bytes.length);
                value = LongBinaries.fromArray(JdbdBuffers.decodeHex(v, v.length));
            } else {
                value = LongBinaries.fromArray(bytes);
            }
        } else {
            value = new String(bytes, rowMeta.clientCharset);
        }
        return value;
    }


    /**
     * @see #readRowData(ByteBuf)
     * @see io.jdbd.postgre.util.PgBinds#decideFormatCode(PgType)
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
            default: {
                // other not support. if change this ,change io.jdbd.postgre.util.PgBinds.decideFormatCode
                final byte[] bytes = new byte[valueLength];
                cumulateBuffer.readBytes(bytes);
                value = bytes;
            }

        }
        return value;
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





    static JdbdSQLException createResponseBinaryColumnValueError(final ByteBuf cumulateBuffer
            , final int valueLength, final PgColumnMeta meta) {
        byte[] bytes = new byte[valueLength];
        cumulateBuffer.readBytes(bytes);
        String m = String.format("Server response binary value[%s] error for PgColumnMeta[%s]."
                , Arrays.toString(bytes), meta);
        return new JdbdSQLException(new SQLException(m));
    }


    enum Phase {
        READ_ROW_META,
        READ_ROWS,
        READ_RESULT_TERMINATOR,
        END
    }

}
