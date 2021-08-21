package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.meta.NullMode;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgType;
import io.jdbd.result.FieldType;
import io.jdbd.result.ResultRowMeta;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.sql.SQLException;
import java.util.*;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
 */
final class PgRowMeta implements ResultRowMeta {

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
     */
    static PgRowMeta read(ByteBuf message, StmtTask stmtTask) {
        PgColumnMeta[] columnMetaArray = PgColumnMeta.read(message, stmtTask.adjutant());
        return new PgRowMeta(stmtTask.getAndIncrementResultIndex(), columnMetaArray);
    }

    final int resultIndex;

    final PgColumnMeta[] columnMetaArray;

    private final Map<String, Integer> labelToIndexMap;

    // don't need volatile
    private List<String> labelList;

    private PgRowMeta(int resultIndex, final PgColumnMeta[] columnMetaArray) {
        if (resultIndex < 0) {
            throw new IllegalArgumentException(String.format("resultIndex[%s] less than 0 .", resultIndex));
        }
        this.resultIndex = resultIndex;
        this.columnMetaArray = columnMetaArray;

        if (columnMetaArray.length > 30) {
            this.labelToIndexMap = createLabelToIndexMap(columnMetaArray);
        } else {
            this.labelToIndexMap = Collections.emptyMap();
        }
    }

    @Override
    public final int getResultIndex() {
        return this.resultIndex;
    }

    @Override
    public final int getColumnCount() {
        return this.columnMetaArray.length;
    }

    @Override
    public final FieldType getFieldType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final FieldType getFieldType(int indexBaseZero) {
        // have to return UNKNOWN,because postgre RowDescription message can't return table name ,column name etc.
        return FieldType.UNKNOWN;
    }

    @Override
    public final FieldType getFieldType(String columnLabel) {
        return getFieldType(getColumnIndex(columnLabel));
    }

    @Override
    public final List<String> getColumnLabelList() {
        List<String> labelList = this.labelList;
        if (labelList != null) {
            return labelList;
        }
        final PgColumnMeta[] columnMetaArray = this.columnMetaArray;
        if (columnMetaArray.length == 1) {
            labelList = Collections.singletonList(columnMetaArray[0].columnLabel);
        } else {
            labelList = new ArrayList<>(columnMetaArray.length);
            for (PgColumnMeta meta : columnMetaArray) {
                labelList.add(meta.columnLabel);
            }
            labelList = Collections.unmodifiableList(labelList);
        }
        this.labelList = labelList;
        return labelList;
    }

    @Override
    public final String getColumnLabel(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].columnLabel;
    }

    @Override
    public final int getColumnIndex(final String columnLabel) throws JdbdSQLException {
        Objects.requireNonNull(columnLabel, "columnLabel");

        if (!this.labelToIndexMap.isEmpty()) {
            final Integer columnIndex = this.labelToIndexMap.get(columnLabel);
            if (columnIndex == null) {
                throw createNotFoundIndexException(columnLabel);
            }
            return columnIndex;
        }

        int indexBaseZero = -1;
        final PgColumnMeta[] columnMetaArray = this.columnMetaArray;
        for (int i = 0; i < columnMetaArray.length; i++) {
            if (columnLabel.equals(columnMetaArray[i].columnLabel)) {
                indexBaseZero = i;
                break;
            }
        }
        if (indexBaseZero < 0) {
            throw createNotFoundIndexException(columnLabel);
        }
        return indexBaseZero;
    }

    @Override
    public final PgType getSQLType(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].pgType;
    }

    @Override
    public SQLType getSQLType(String columnAlias) throws JdbdSQLException {
        return getSQLType(getColumnIndex(columnAlias));
    }

    @Override
    public final NullMode getNullMode(int indexBaseZero) throws JdbdSQLException {
        return NullMode.UNKNOWN;
    }

    @Override
    public final NullMode getNullMode(String columnAlias) throws JdbdSQLException {
        return NullMode.UNKNOWN;
    }


    @Override
    public final int getPrecision(final int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].getPrecision();
    }

    @Override
    public final int getPrecision(String columnAlias) throws JdbdSQLException {
        return getPrecision(getColumnIndex(columnAlias));
    }

    @Override
    public final int getScale(final int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].getScale();
    }

    @Override
    public final int getScale(String columnAlias) throws JdbdSQLException {
        return getScale(getColumnIndex(columnAlias));
    }

    @Override
    public final boolean isCaseSensitive(final int indexBaseZero) throws JdbdSQLException {
        final boolean sensitive;
        switch (this.columnMetaArray[checkIndex(indexBaseZero)].pgType) {
            case OID:
            case SMALLINT:
            case SMALLINT_ARRAY:
            case INTEGER:
            case INTEGER_ARRAY:
            case BIGINT:
            case BIGINT_ARRAY:
            case REAL:
            case REAL_ARRAY:
            case DOUBLE:
            case DOUBLE_ARRAY:
            case DECIMAL:
            case DECIMAL_ARRAY:
            case BOOLEAN:
            case BOOLEAN_ARRAY:
            case BIT:
            case BIT_ARRAY:
            case VARBIT:
            case VARBIT_ARRAY:
            case TIMESTAMP:
            case TIMESTAMP_ARRAY:
            case TIME:
            case TIME_ARRAY:
            case DATE:
            case DATE_ARRAY:
            case TIMESTAMPTZ:
            case TIMESTAMPTZ_ARRAY:
            case TIMETZ:
            case TIMETZ_ARRAY:
            case INTERVAL:
            case INTERVAL_ARRAY:
            case POINT:
            case POINT_ARRAY:
            case BOX:
            case BOX_ARRAY:
            case LINE:
            case LINE_ARRAY:
            case LINE_SEGMENT:
            case LINE_SEGMENT_ARRAY:
            case POLYGON:
            case POLYGON_ARRAY:
            case CIRCLE:
            case CIRCLES_ARRAY:
            case UUID:
            case UUID_ARRAY:
            case CIDR:
            case CIDR_ARRAY:
            case INET:
            case INET_ARRAY:
            case INT4RANGE:
            case INT4RANGE_ARRAY:
            case INT8RANGE:
            case INT8RANGE_ARRAY:
                sensitive = false;
                break;
            default:
                sensitive = true;
        }
        return sensitive;
    }

    @Override
    public final boolean isCaseSensitive(String columnLabel) throws JdbdSQLException {
        return isCaseSensitive(getColumnIndex(columnLabel));
    }

    @Nullable
    @Override
    public final String getCatalogName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Nullable
    @Override
    public final String getSchemaName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Nullable
    @Override
    public final String getTableName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Nullable
    @Override
    public final String getColumnName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    private int checkIndex(final int indexBasedZero) {
        if (indexBasedZero < 0 || indexBasedZero >= this.columnMetaArray.length) {
            String m = String.format("Invalid column index[%s] ,should be [0,%s)."
                    , indexBasedZero, this.columnMetaArray.length);
            throw new JdbdSQLException(new SQLException(m));
        }
        return indexBasedZero;
    }

    /*################################## blow private static method ##################################*/

    private static JdbdException createNotFoundIndexException(final String columnLabel) {
        String m = String.format("Not found column index for column label[%s]", columnLabel);
        return new JdbdSQLException(new SQLException(m));
    }

    /**
     * @return a unmodifiable map
     */
    private static Map<String, Integer> createLabelToIndexMap(final PgColumnMeta[] columnMetaArray) {
        final Map<String, Integer> map;
        if (columnMetaArray.length == 1) {
            map = Collections.singletonMap(columnMetaArray[0].columnLabel, 0);
        } else {
            Map<String, Integer> tempMap = new HashMap<>((int) (columnMetaArray.length / 0.75f));
            for (int i = 0; i < columnMetaArray.length; i++) {
                tempMap.putIfAbsent(columnMetaArray[i].columnLabel, i);
            }
            map = Collections.unmodifiableMap(tempMap);
        }
        return map;
    }


}
