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

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
 */
final class PgRowMeta implements ResultRowMeta {

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
     */
    static PgRowMeta read(ByteBuf message, StmtTask stmtTask) {
        TaskAdjutant adjutant = stmtTask.adjutant();
        PgColumnMeta[] columnMetaArray = PgColumnMeta.read(message, adjutant);
        return new PgRowMeta(stmtTask.getAndIncrementResultIndex(), columnMetaArray, adjutant);
    }

    static PgRowMeta readForPrepare(ByteBuf message, TaskAdjutant adjutant) {
        PgColumnMeta[] columnMetaArray = PgColumnMeta.read(message, adjutant);
        return new PgRowMeta(-1, columnMetaArray, adjutant);
    }

    final int resultIndex;

    final PgColumnMeta[] columnMetaArray;

    private final Map<String, Integer> labelToIndexMap;

    //if non-null,then don't invoke any setXxx() method again after constructor.
    final DecimalFormat moneyFormat;

    // cache client charset,because possibly is changed in multi-statement.
    final Charset clientCharset;

    // don't need volatile
    private List<String> labelList;

    private PgRowMeta(int resultIndex, final PgColumnMeta[] columnMetaArray, TaskAdjutant adjutant) {
        this.resultIndex = resultIndex;
        this.columnMetaArray = columnMetaArray;

        if (columnMetaArray.length < 30) {
            this.labelToIndexMap = Collections.emptyMap();
        } else {
            this.labelToIndexMap = createLabelToIndexMap(columnMetaArray);
        }
        this.moneyFormat = createMoneyFormatIfNeed(columnMetaArray, adjutant);
        this.clientCharset = adjutant.clientCharset();
    }

    @Override
    public final int getResultIndex() {
        final int resultIndex = this.resultIndex;
        if (resultIndex < 0) {
            throw new UnsupportedOperationException("Only used by session bind parameters.");
        }
        return resultIndex;
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
    public final boolean isAutoIncrement(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public final boolean isAutoIncrement(String columnAlias) throws JdbdSQLException {
        return isAutoIncrement(getColumnIndex(columnAlias));
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

    @Nullable
    private static DecimalFormat createMoneyFormatIfNeed(final PgColumnMeta[] columnMetaArray
            , final TaskAdjutant adjutant) {
        DecimalFormat format = null;
        for (PgColumnMeta meta : columnMetaArray) {
            if (meta.pgType != PgType.MONEY && meta.pgType != PgType.MONEY_ARRAY) {
                continue;
            }
            final NumberFormat f = NumberFormat.getCurrencyInstance(adjutant.server().moneyLocal());
            if (f instanceof DecimalFormat) {
                format = (DecimalFormat) f;
                format.setParseBigDecimal(true); // must be true.
            }
            break;
        }
        return format;
    }


}
