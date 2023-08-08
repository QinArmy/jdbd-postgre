package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.type.PgGeometries;
import io.jdbd.postgre.util.PgArrays;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.RefCursor;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.type.Point;
import io.jdbd.vendor.result.ColumnConverts;
import io.jdbd.vendor.result.ColumnMeta;
import io.jdbd.vendor.result.VendorDataRow;
import io.jdbd.vendor.util.JdbdStrings;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.IntFunction;

final class PgResultSetReader implements ResultSetReader {

    static PgResultSetReader create(StmtTask task) {
        return new PgResultSetReader(task);
    }

    private static final Logger LOG = LoggerFactory.getLogger(PgResultSetReader.class);

    private static final Path TEMP_DIRECTORY = Paths.get(System.getProperty("java.io.tmpdir"), "jdbd/postgre/big_row")
            .toAbsolutePath();

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


    private final StmtTask task;

    private final TaskAdjutant adjutant;

    private MutableCurrentRow currentRow;


    private PgResultSetReader(StmtTask task) {
        this.task = task;
        this.adjutant = task.adjutant();
    }

    @Override
    public boolean read(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer)
            throws JdbdException {
        MutableCurrentRow currentRow = this.currentRow;
        if (currentRow == null) {
            final StmtTask task = this.task;
            this.currentRow = currentRow = new MutableCurrentRow(PgRowMeta.read(cumulateBuffer, task));
            if (!task.isCancelled()) {
                task.next(currentRow.rowMeta); // emit io.jdbd.result.ResultRowMeta
            }
        }
        final boolean resultSetEnd;
        resultSetEnd = readRowData(cumulateBuffer);
        if (resultSetEnd) {
            this.task.readResultStateOfQuery(cumulateBuffer, currentRow::getResultNo);
            // reset this instance
            this.currentRow = null;
        }
        return resultSetEnd;
    }

    /**
     * @return true : read row data end.
     * @see #read(ByteBuf, Consumer)
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">DataRow (B)</a>
     */
    private boolean readRowData(final ByteBuf cumulateBuffer) {
        final MutableCurrentRow currentRow = this.currentRow;
        assert currentRow != null;
        final boolean traceEnabled = LOG.isTraceEnabled();
        if (traceEnabled) {
            LOG.trace("Read ResultSet row data for {}", currentRow);
        }
        final StmtTask sink = this.task;
        final PgRowMeta rowMeta = currentRow.rowMeta;
        final PgColumnMeta[] columnMetaArray = rowMeta.columnMetaArray;
        final Object[] columnArray = currentRow.columnArray;

        final int columnCount = columnMetaArray.length;

        boolean isCanceled = sink.isCancelled();
        PgColumnMeta meta;
        for (int msgIndex, nextMsgIndex; Messages.hasOneMessage(cumulateBuffer); ) {
            msgIndex = cumulateBuffer.readerIndex();

            if (cumulateBuffer.getByte(msgIndex) != Messages.D) {
                if (traceEnabled) {
                    LOG.trace("Read ResultSet end for result No : {}", currentRow.getResultNo());
                }
                return true;
            }
            cumulateBuffer.readByte(); // skip message type byte
            nextMsgIndex = msgIndex + 1 + cumulateBuffer.readInt();

            if (isCanceled) { // downstream cancel or occur error.
                cumulateBuffer.readerIndex(nextMsgIndex);// skip row
                continue;
            }
            if (cumulateBuffer.readShort() != columnCount) {
                String m = String.format("Server RowData message column count[%s] and RowDescription[%s] not match.",
                        cumulateBuffer.getShort(cumulateBuffer.readerIndex() - 2), columnCount);
                throw new JdbdException(m);
            }

            for (int i = 0, valueLength; i < columnCount; i++) {
                valueLength = cumulateBuffer.readInt();
                if (valueLength == -1) {
                    // -1 indicates a NULL column value.
                    columnArray[i] = null;
                    continue;
                }
                meta = columnMetaArray[i];
                if (meta.textFormat) {
                    columnArray[i] = readColumnFromText(cumulateBuffer, valueLength, rowMeta, meta);
                } else {
                    columnArray[i] = readColumnFromBinary(cumulateBuffer, valueLength, meta);
                }
            }

            currentRow.rowCount++;
            sink.next(currentRow);

            cumulateBuffer.readerIndex(nextMsgIndex);// avoid to tailor filler.
        }

        return false;
    }


    private Object readColumnFromText(final ByteBuf cumulateBuffer, final int valueLength, final PgRowMeta rowMeta,
                                      final PgColumnMeta meta) {

        final DataType dataType = meta.dataType;

        int startIndex = cumulateBuffer.readerIndex();
        final byte[] valueBytes;
        if (dataType == PgType.BYTEA
                && cumulateBuffer.getByte(startIndex++) == PgConstant.BACK_SLASH
                && cumulateBuffer.getByte(startIndex) == 'x') {
            valueBytes = new byte[valueLength - 2];
            cumulateBuffer.skipBytes(2);
        } else {
            valueBytes = new byte[valueLength];
        }

        if (valueBytes.length != 0) {
            cumulateBuffer.readBytes(valueBytes);
        }


        if (!(dataType instanceof PgType) || dataType.isArray()) {
            return new String(valueBytes, rowMeta.clientCharset);
        }

        final Object columnValue;
        switch ((PgType) dataType) {
            case BOOLEAN: {
                if (valueLength != 1) {
                    throw columnValueError(meta);
                }
                columnValue = readBoolean(valueBytes[0], meta);
            }
            break;
            case SMALLINT:
                columnValue = Short.parseShort(new String(valueBytes, rowMeta.clientCharset));
                break;
            case INTEGER:
                columnValue = Integer.parseInt(new String(valueBytes, rowMeta.clientCharset));
                break;
            case OID:
            case BIGINT:
                columnValue = Long.parseLong(new String(valueBytes, rowMeta.clientCharset));
                break;
            case DECIMAL:
                columnValue = new BigDecimal(new String(valueBytes, rowMeta.clientCharset));
                break;
            case REAL:
                columnValue = Float.parseFloat(new String(valueBytes, rowMeta.clientCharset));
                break;
            case FLOAT8:
                columnValue = Double.parseDouble(new String(valueBytes, rowMeta.clientCharset));
                break;

            case BYTEA:
                columnValue = valueBytes;
                break;

            case CHAR:
            case VARCHAR:
            case TEXT:
            case TSQUERY:
            case TSVECTOR:

            case TIME:
            case TIMETZ:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMPTZ:
            case INTERVAL:

            case INT4RANGE:
            case INT8RANGE:
            case NUMRANGE:
            case DATERANGE:
            case TSRANGE:
            case TSTZRANGE:

            case INT4MULTIRANGE:
            case INT8MULTIRANGE:
            case NUMMULTIRANGE:
            case DATEMULTIRANGE:
            case TSMULTIRANGE:
            case TSTZMULTIRANGE:

            case JSON:
            case JSONB:
            case JSONPATH:
            case XML:

            case POINT:
            case LINE:
            case PATH:
            case CIRCLE:
            case BOX:
            case POLYGON:
            case LSEG:

            case CIDR:
            case INET:
            case MACADDR:
            case MACADDR8:

            case MONEY:
            case UUID:

            case BIT:
            case VARBIT:

            case UNSPECIFIED:
                columnValue = new String(valueBytes, rowMeta.clientCharset);
                break;
            case REF_CURSOR: {
                final String cursorName = new String(valueBytes, rowMeta.clientCharset);
                columnValue = PgRefCursor.cursorOfColumn(cursorName, meta, this.adjutant);
            }
            break;
            case REF_CURSOR_ARRAY: {
                final String source = new String(valueBytes, rowMeta.clientCharset);

                final int arrayDimension;
                arrayDimension = ColumnArrays.readArrayDimension(source);

                final Class<?> stringArrayClass;
                stringArrayClass = PgArrays.arrayClassOf(String.class, arrayDimension);

                final Object stringArray;
                stringArray = ColumnArrays.parseArray(source, meta, rowMeta, stringArrayClass);

                columnValue = new RefCursorArray(source, convertToRefCursorArray(stringArray, meta));
            }
            break;
            default:
                throw PgExceptions.unexpectedEnum((PgType) dataType);

        }
        return columnValue;
    }


    /**
     * @see #readColumnFromText(ByteBuf, int, PgRowMeta, PgColumnMeta)
     */
    private Object convertToRefCursorArray(final Object stringArray, final PgColumnMeta meta) {
        final int dimension, length;
        dimension = PgArrays.dimensionOf(stringArray.getClass());
        length = Array.getLength(stringArray);

        final boolean oneDimension;
        oneDimension = dimension == 1;

        final Object cursorArray;
        cursorArray = Array.newInstance(RefCursor.class, dimension);

        Object stringElement;
        for (int i = 0; i < length; i++) {
            stringElement = Array.get(stringArray, i);

            if (stringElement == null) {
                Array.set(cursorArray, i, null);
            } else if (oneDimension) {
                Array.set(cursorArray, i, PgRefCursor.cursorOfColumn((String) stringElement, meta, this.adjutant));
            } else {
                Array.set(cursorArray, i, convertToRefCursorArray(stringElement, meta));
            }
        }
        return cursorArray;
    }


    /**
     * @see #readRowData(ByteBuf)
     * @see io.jdbd.postgre.util.PgBinds#decideFormatCode(PgType)
     */
    private Object readColumnFromBinary(final ByteBuf cumulateBuffer, final int valueLength, final PgColumnMeta meta) {
        final DataType dataType = meta.dataType;
        if (!(dataType instanceof PgType)) {
            throw unexpectedBinaryFormat(dataType);
        }
        final Object columnValue;
        switch ((PgType) dataType) {
            case BOOLEAN: {
                if (valueLength != 1) {
                    throw binaryFormatLengthError(dataType, valueLength);
                }
                columnValue = readBoolean(cumulateBuffer.readByte(), meta);
            }
            break;
            case SMALLINT: {
                if (valueLength != 2) {
                    throw binaryFormatLengthError(dataType, valueLength);
                }
                columnValue = cumulateBuffer.readShort();
            }
            break;
            case INTEGER: {
                if (valueLength != 4) {
                    throw binaryFormatLengthError(dataType, valueLength);
                }
                columnValue = cumulateBuffer.readInt();
            }
            break;
            case BIGINT: {
                if (valueLength != 8) {
                    throw binaryFormatLengthError(dataType, valueLength);
                }
                columnValue = cumulateBuffer.readInt();
            }
            break;
            case OID: {
                switch (valueLength) {
                    case 1:
                        columnValue = (long) cumulateBuffer.readByte();
                        break;
                    case 2:
                        columnValue = (long) cumulateBuffer.readShort();
                        break;
                    case 4:
                        columnValue = (long) cumulateBuffer.readInt();
                        break;
                    case 8:
                        columnValue = cumulateBuffer.readLong();
                        break;
                    default:
                        throw binaryFormatLengthError(dataType, valueLength);
                }
            }
            break;
            case REAL: {
                if (valueLength != 4) {
                    throw binaryFormatLengthError(dataType, valueLength);
                }
                columnValue = Float.intBitsToFloat(cumulateBuffer.readInt());
            }
            break;
            case FLOAT8: {
                if (valueLength != 8) {
                    throw binaryFormatLengthError(dataType, valueLength);
                }
                columnValue = Double.longBitsToDouble(cumulateBuffer.readLong());
            }
            break;
            case BYTEA: {
                final byte[] valueBytes = new byte[valueLength];
                if (valueLength != 0) {
                    cumulateBuffer.readBytes(valueBytes);
                }
                columnValue = valueBytes;
            }
            break;
            default:
                throw unexpectedBinaryFormat(dataType);
        }
        return columnValue;
    }



    /*-------------------below static method -------------------*/

    /**
     * @see #readColumnFromText(ByteBuf, int, PgRowMeta, PgColumnMeta)
     * @see #readColumnFromBinary(ByteBuf, int, PgColumnMeta)
     */
    private static Boolean readBoolean(final byte valueByte, final PgColumnMeta meta) {
        final Boolean value;
        switch (valueByte) {
            case 't':
                value = Boolean.TRUE;
                break;
            case 'f':
                value = Boolean.FALSE;
                break;
            default:
                throw columnValueError(meta);
        }
        return value;
    }


    /**
     * @see PgDataRow#get(int)
     */
    private static LocalTime parseLocalTime(final String source, final PgColumnMeta meta, final DateStyle style) {
        // currently jdbd-postgre support only iso style,see io.jdbd.postgre.protocol.client.PgConnectionTask
        return LocalTime.parse(source, PgTimes.TIME_FORMATTER_6);
    }

    /**
     * @see PgDataRow#get(int)
     */
    private static OffsetTime parseOffsetTime(final String source, final PgColumnMeta meta, final DateStyle style) {
        // currently jdbd-postgre support only iso style,see io.jdbd.postgre.protocol.client.PgConnectionTask
        return OffsetTime.parse(source, PgTimes.OFFSET_TIME_FORMATTER_6);
    }

    /**
     * @see PgDataRow#get(int)
     */
    private static LocalDate parseLocalDate(final String source, final PgColumnMeta meta, final ServerEnv env) {
        // currently jdbd-postgre support only iso style,see io.jdbd.postgre.protocol.client.PgConnectionTask
        return LocalDate.parse(source);
    }

    /**
     * @see PgDataRow#get(int)
     */
    private static LocalDateTime parseLocalDateTime(final String source, final PgColumnMeta meta, final ServerEnv env) {
        // currently jdbd-postgre support only iso style,see io.jdbd.postgre.protocol.client.PgConnectionTask
        return LocalDateTime.parse(source, PgTimes.DATETIME_FORMATTER_6);
    }

    /**
     * @see PgDataRow#get(int, Class)
     */
    private static OffsetDateTime parseOffsetDateTime(final String source, final PgColumnMeta meta, final ServerEnv env) {
        // currently jdbd-postgre support only iso style,see io.jdbd.postgre.protocol.client.PgConnectionTask
        return OffsetDateTime.parse(source, PgTimes.OFFSET_DATETIME_FORMATTER_6);
    }


//    /**
//     * @see #parseColumnFromBinary(ByteBuf, int, PgColumnMeta)
//     */
//    private OffsetDateTime parseLocalDateTimeFromBinaryLong(final ByteBuf cumulateBuffer) {
//        final long seconds;
//        final int nanos;
//
//        final long time = cumulateBuffer.readLong();
//        if (time == Long.MAX_VALUE) {
//            seconds = DATE_POSITIVE_INFINITY / 1000;
//            nanos = 0;
//        } else if (time == Long.MIN_VALUE) {
//            seconds = DATE_NEGATIVE_INFINITY / 1000;
//            nanos = 0;
//        } else {
//            final int million = 1000_000;
//            long secondPart = time / million;
//            int nanoPart = (int) (time - secondPart * million);
//            if (nanoPart < 0) {
//                secondPart--;
//                nanoPart += million;
//            }
//            nanoPart *= 1000;
//
//            seconds = PgTimes.toJavaSeconds(secondPart);
//            nanos = nanoPart;
//        }
//        return OffsetDateTime.of(LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC), ZoneOffset.UTC);
//    }

//    /**
//     * @see #parseColumnFromBinary(ByteBuf, int, PgColumnMeta)
//     */
//    private OffsetDateTime parseLocalDateTimeFromBinaryDouble(final ByteBuf cumulateBuffer) {
//        final long seconds;
//        final int nanos;
//
//        final double time = Double.longBitsToDouble(cumulateBuffer.readLong());
//        if (time == Double.POSITIVE_INFINITY) {
//            seconds = DATE_POSITIVE_INFINITY / 1000;
//            nanos = 0;
//        } else if (time == Double.NEGATIVE_INFINITY) {
//            seconds = DATE_NEGATIVE_INFINITY / 1000;
//            nanos = 0;
//        } else {
//            final int million = 1000_000;
//            long secondPart = (long) time;
//            int nanoPart = (int) ((time - secondPart) * million);
//            if (nanoPart < 0) {
//                secondPart--;
//                nanoPart += million;
//            }
//            nanoPart *= 1000;
//
//            seconds = PgTimes.toJavaSeconds(secondPart);
//            nanos = nanoPart;
//        }
//        return OffsetDateTime.of(LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC), ZoneOffset.UTC);
//    }


//    static JdbdException createResponseBinaryColumnValueError(final ByteBuf cumulateBuffer
//            , final int valueLength, final PgColumnMeta meta) {
//        byte[] bytes = new byte[valueLength];
//        cumulateBuffer.readBytes(bytes);
//        String m = String.format("Server response binary value[%s] error for PgColumnMeta[%s]."
//                , Arrays.toString(bytes), meta);
//        return new JdbdException(m);
//    }

    private static JdbdException columnValueError(PgColumnMeta meta) {
        String m = String.format("server response column value for %s error", meta);
        return new JdbdException(m);
    }

    private static JdbdException unexpectedBinaryFormat(DataType dataType) {
        String m = String.format("server response unexpected binary format for %s", dataType);
        return new JdbdException(m);
    }

    private static JdbdException binaryFormatLengthError(DataType dataType, int length) {
        String m = String.format("server response error binary format length[%s] for %s", length, dataType);
        return new JdbdException(m);
    }


    private static final class RefCursorArray {

        private final String source;

        private final Object cursorArray;

        private RefCursorArray(String source, Object cursorArray) {
            this.source = source;
            this.cursorArray = cursorArray;
        }


    }// RefCursorArray


    private static abstract class PgDataRow extends VendorDataRow {

        final PgRowMeta rowMeta;

        final Object[] columnArray;


        private PgDataRow(PgRowMeta rowMeta) {
            this.rowMeta = rowMeta;
            final int arrayLength;
            arrayLength = rowMeta.columnMetaArray.length;
            this.columnArray = new Object[arrayLength];
        }

        private PgDataRow(PgCurrentRow currentRow) {
            this.rowMeta = currentRow.rowMeta;

            final Object[] columnArray = new Object[currentRow.columnArray.length];
            System.arraycopy(currentRow.columnArray, 0, columnArray, 0, columnArray.length);

            this.columnArray = columnArray;
        }

        @Override
        public final ResultRowMeta getRowMeta() {
            return this.rowMeta;
        }

        @Override
        public final int getResultNo() {
            return this.rowMeta.resultNo;
        }

        @Override
        public final boolean isBigColumn(int indexBasedZero) throws JdbdException {
            this.rowMeta.checkIndex(indexBasedZero);
            // always false,because postgre protocol don't need.
            return false;
        }

        @Override
        public final boolean isBigRow() {
            // always false,because postgre protocol don't need.
            return false;
        }

        @Override
        public final Object get(final int indexBasedZero) throws JdbdException {
            final PgRowMeta rowMeta = this.rowMeta;
            final Object source;
            source = this.columnArray[rowMeta.checkIndex(indexBasedZero)];
            if (source == null) {
                return null;
            }

            final PgColumnMeta meta = rowMeta.columnMetaArray[indexBasedZero];
            final DataType dataType = meta.dataType;
            if (!(dataType instanceof PgType) || dataType.isArray()) {
                return source;
            }
            try {
                final Object columnValue;
                switch ((PgType) dataType) {
                    case TIME:
                        columnValue = parseLocalTime((String) source, meta, rowMeta.serverEnv.dateStyle());
                        break;
                    case TIMETZ:
                        columnValue = parseOffsetTime((String) source, meta, rowMeta.serverEnv.dateStyle());
                        break;
                    case DATE:
                        columnValue = parseLocalDate((String) source, meta, rowMeta.serverEnv);
                        break;
                    case TIMESTAMP:
                        columnValue = parseLocalDateTime((String) source, meta, rowMeta.serverEnv);
                        break;
                    case TIMESTAMPTZ:
                        columnValue = parseOffsetDateTime((String) source, meta, rowMeta.serverEnv);
                        break;
                    case BIT:
                    case VARBIT:
                        columnValue = JdbdStrings.bitStringToBitSet((String) source, true);
                        break;
                    case UUID:
                        columnValue = UUID.fromString((String) source);
                        break;
                    case POINT:
                        columnValue = PgGeometries.point((String) source);
                        break;
                    case REF_CURSOR_ARRAY:
                        // postgre array first java type always is String.class
                        columnValue = ((RefCursorArray) source).source;
                        break;
                    default:
                        columnValue = source;
                }
                return columnValue;
            } catch (JdbdException e) {
                throw e;
            } catch (Throwable e) {
                throw PgExceptions.cannotConvertColumnValue(meta, source, ((PgType) dataType).firstJavaType(), e);
            }

        }


        @Override
        public final <T> T get(final int indexBasedZero, final Class<T> columnClass) throws JdbdException {
            final PgRowMeta rowMeta = this.rowMeta;
            final Object source;
            source = this.columnArray[rowMeta.checkIndex(indexBasedZero)];
            if (source == null) {
                return null;
            }
            final PgColumnMeta meta = rowMeta.columnMetaArray[indexBasedZero];
            final DataType dataType = meta.dataType;
            try {
                final T columnValue;
                if (dataType == PgType.REF_CURSOR_ARRAY) {
                    columnValue = convertRefCursorArray(source, meta, columnClass);
                } else if (dataType.isArray()) {
                    if (!columnClass.isArray()
                            || (dataType == PgType.BYTEA_ARRAY && PgArrays.dimensionOf(columnClass) < 2)) {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, columnClass, null);
                    }
                    columnValue = ColumnArrays.parseArray((String) source, meta, rowMeta, columnClass);
                } else if (dataType instanceof PgType) {
                    columnValue = convertSimpleColumn((PgType) dataType, source, meta, columnClass);
                } else {
                    //TODO add convertor for Composite Types
                    columnValue = ColumnConverts.convertToTarget(meta, source, columnClass, rowMeta.serverEnv.serverZone());
                }
                return columnValue;
            } catch (JdbdException e) {
                throw e;
            } catch (Throwable e) {
                throw PgExceptions.cannotConvertColumnValue(meta, source, columnClass, e);
            }
        }


        @Override
        public final <T> List<T> getList(final int indexBasedZero, final Class<T> elementClass,
                                         final IntFunction<List<T>> constructor) throws JdbdException {
            return null;
        }

        @Override
        public final <T> Set<T> getSet(int indexBasedZero, Class<T> elementClass, IntFunction<Set<T>> constructor)
                throws JdbdException {
            throw new JdbdException("jdbd-postgre don't support getSet() method");
        }

        @Override
        public final <K, V> Map<K, V> getMap(int indexBasedZero, Class<K> keyClass, Class<V> valueClass,
                                             IntFunction<Map<K, V>> constructor) throws JdbdException {
            //TODO add hstore
            throw new JdbdException("jdbd-postgre don't support getMap() method");
        }

        @Override
        public final <T> Publisher<T> getPublisher(int indexBasedZero, Class<T> valueClass) throws JdbdException {
            return null;
        }


        @Override
        protected final ColumnMeta getColumnMeta(int safeIndex) {
            return this.rowMeta.columnMetaArray[safeIndex];
        }


        /**
         * @see #get(int, Class)
         */
        @SuppressWarnings("unchecked")
        private <T> T convertRefCursorArray(final Object source, final PgColumnMeta meta, final Class<T> columnClass) {
            final T columnValue;
            final Class<?> componentClass;
            if (columnClass == String.class) {
                columnValue = (T) ((RefCursorArray) source).source;
            } else if (!columnClass.isArray()) {
                throw PgExceptions.cannotConvertColumnValue(meta, source, columnClass, null);
            } else if ((componentClass = PgArrays.underlyingComponent(columnClass)) == RefCursor.class) {
                columnValue = (T) ((RefCursorArray) source).cursorArray;
            } else if (componentClass == String.class) {
                columnValue = ColumnArrays.parseArray(((RefCursorArray) source).source, meta, rowMeta, columnClass);
            } else {
                throw PgExceptions.cannotConvertColumnValue(meta, source, columnClass, null);
            }
            return columnValue;
        }

        @SuppressWarnings("unchecked")
        private <T> T convertSimpleColumn(final PgType dataType, final Object source, final PgColumnMeta meta,
                                          final Class<T> columnClass) {

            final Object columnValue;
            switch (dataType) {
                case TIME: {
                    if (columnClass == String.class) {
                        columnValue = source;
                    } else if (columnClass == LocalTime.class) {
                        columnValue = parseLocalTime((String) source, meta, this.rowMeta.serverEnv.dateStyle());
                    } else {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, columnClass, null);
                    }
                }
                break;
                case TIMETZ: {
                    if (columnClass == String.class) {
                        columnValue = source;
                    } else if (columnClass == OffsetTime.class) {
                        columnValue = parseOffsetTime((String) source, meta, this.rowMeta.serverEnv.dateStyle());
                    } else {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, columnClass, null);
                    }
                }
                break;
                case DATE: {
                    if (columnClass == String.class) {
                        columnValue = source;
                    } else {
                        final LocalDate v;
                        v = parseLocalDate((String) source, meta, this.rowMeta.serverEnv);
                        if (columnClass == LocalDate.class) {
                            columnValue = v;
                        } else {
                            columnValue = ColumnConverts.convertToTarget(meta, v, columnClass, null);
                        }
                    }
                }
                break;
                case TIMESTAMP: {
                    if (columnClass == String.class) {
                        columnValue = source;
                    } else {
                        final LocalDateTime v;
                        v = parseLocalDateTime((String) source, meta, this.rowMeta.serverEnv);
                        if (columnClass == LocalDateTime.class) {
                            columnValue = v;
                        } else {
                            columnValue = ColumnConverts.convertToTarget(meta, v, columnClass, null);
                        }
                    }
                }
                break;
                case TIMESTAMPTZ: {
                    if (columnClass == String.class) {
                        columnValue = source;
                    } else {
                        final OffsetDateTime v;
                        v = parseOffsetDateTime((String) source, meta, rowMeta.serverEnv);
                        if (columnClass == OffsetDateTime.class) {
                            columnValue = v;
                        } else {
                            columnValue = ColumnConverts.convertToTarget(meta, v, columnClass, null);
                        }
                    }
                }
                break;
                case BIT:
                case VARBIT: {
                    if (columnClass == String.class) {
                        columnValue = source;
                    } else if (columnClass == BitSet.class) {
                        columnValue = JdbdStrings.bitStringToBitSet((String) source, true);
                    } else {
                        columnValue = ColumnConverts.convertToTarget(meta, source, columnClass, null);
                    }
                }
                break;
                case UUID: {
                    if (columnClass == String.class) {
                        columnValue = source;
                    } else if (columnClass == UUID.class) {
                        columnValue = UUID.fromString((String) source);
                    } else {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, columnClass, null);
                    }
                }
                break;
                case POINT: {
                    if (columnClass == String.class) {
                        columnValue = source;
                    } else if (columnClass == Point.class) {
                        columnValue = PgGeometries.point((String) source);
                    } else {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, columnClass, null);
                    }
                }
                break;
                case REF_CURSOR: {
                    if (columnClass == String.class) {
                        columnValue = ((PgRefCursor) source).name();
                    } else if (columnClass == RefCursor.class) {
                        columnValue = source;
                    } else {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, columnClass, null);
                    }
                }
                break;
                default:
                    columnValue = ColumnConverts.convertToTarget(meta, source, columnClass, null);
            }
            return (T) columnValue;
        }


    }//PgDataRow


    private static abstract class PgCurrentRow extends PgDataRow implements CurrentRow {

        private PgCurrentRow(PgRowMeta rowMeta) {
            super(rowMeta);
        }

        private PgCurrentRow(MutableCurrentRow currentRow) {
            super(currentRow);
        }


        @Override
        public final ResultRow asResultRow() {
            return new PgResultRow(this);
        }


    }//PgCurrentRow


    private static final class MutableCurrentRow extends PgCurrentRow {

        private long rowCount = 0L;

        private MutableCurrentRow(PgRowMeta rowMeta) {
            super(rowMeta);
        }

        @Override
        public long rowNumber() {
            return this.rowCount;
        }


        @Override
        protected CurrentRow copyCurrentRowIfNeed() {
            return new ImmutableCurrentRow(this);
        }


    }//MutableCurrentRow

    private static final class ImmutableCurrentRow extends PgCurrentRow {

        private final long rowNumber;

        private ImmutableCurrentRow(MutableCurrentRow currentRow) {
            super(currentRow);
            this.rowNumber = currentRow.rowCount;
        }

        @Override
        public long rowNumber() {
            return this.rowNumber;
        }


        @Override
        protected CurrentRow copyCurrentRowIfNeed() {
            return this;
        }


    }//ImmutableCurrentRow

    private static final class PgResultRow extends PgDataRow implements ResultRow {


        private PgResultRow(PgCurrentRow currentRow) {
            super(currentRow);
        }


    }//PgResultRow


}
