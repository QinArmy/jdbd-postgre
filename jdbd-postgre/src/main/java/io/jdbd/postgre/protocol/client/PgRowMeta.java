package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.*;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgNumbers;
import io.jdbd.result.FieldType;
import io.jdbd.vendor.result.VendorResultRowMeta;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
 */
final class PgRowMeta extends VendorResultRowMeta {

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

    private PgRowMeta(int resultNo, final PgColumnMeta[] columnMetaArray, TaskAdjutant adjutant) {
        super(resultNo);
        this.resultIndex = resultNo;
        this.columnMetaArray = columnMetaArray;

        if (columnMetaArray.length < 6) {
            this.labelToIndexMap = Collections.emptyMap();
        } else {
            this.labelToIndexMap = createLabelToIndexMap(columnMetaArray);
        }
        this.moneyFormat = createMoneyFormatIfNeed(columnMetaArray, adjutant);
        this.clientCharset = adjutant.clientCharset();
    }

    @Override
    public int getColumnCount() {
        return this.columnMetaArray.length;
    }

    @Override
    public FieldType getFieldType(final int indexBasedZero) {
        if (this.columnMetaArray[checkIndex(indexBasedZero)].tableOid == 0) {
            return FieldType.EXPRESSION;
        }
        return FieldType.FIELD;
    }


    @Override
    public List<String> getColumnLabelList() {
        List<String> labelList = this.labelList;
        if (labelList != null) {
            return labelList;
        }
        final PgColumnMeta[] columnMetaArray = this.columnMetaArray;
        if (columnMetaArray.length == 1) {
            labelList = Collections.singletonList(columnMetaArray[0].columnLabel);
        } else {
            labelList = PgCollections.arrayList(columnMetaArray.length);
            for (PgColumnMeta meta : columnMetaArray) {
                labelList.add(meta.columnLabel);
            }
            labelList = Collections.unmodifiableList(labelList);
        }
        this.labelList = labelList;
        return labelList;
    }

    @Override
    public String getColumnLabel(final int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].columnLabel;
    }

    @Override
    public int getColumnIndex(final String columnLabel) throws JdbdException {
        if (this.labelToIndexMap.size() > 0) {
            final Integer columnIndex = this.labelToIndexMap.get(columnLabel);
            if (columnIndex == null) {
                throw createNotFoundIndexException(columnLabel);
            }
            return columnIndex;
        }

        int indexBasedZero = -1;
        final PgColumnMeta[] columnMetaArray = this.columnMetaArray;
        for (int i = 0; i < columnMetaArray.length; i++) {
            if (columnLabel.equals(columnMetaArray[i].columnLabel)) {
                indexBasedZero = i;
                break;
            }
        }
        if (indexBasedZero < 0) {
            throw createNotFoundIndexException(columnLabel);
        }
        return indexBasedZero;
    }


    @Override
    public DataType getDataType(final int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].dataType;
    }

    @Override
    public JdbdType getJdbdType(final int indexBasedZero) throws JdbdException {
        final DataType dataType;
        dataType = this.columnMetaArray[checkIndex(indexBasedZero)].dataType;
        if (dataType instanceof PgType) {
            return ((PgType) dataType).jdbdType();
        }
        return ((PgUserType) dataType).jdbdType;
    }


    @Override
    public KeyMode getKeyMode(final int indexBasedZero) throws JdbdException {
        return KeyMode.UNKNOWN;
    }

    @Override
    public Class<?> getFirstJavaType(final int indexBasedZero) throws JdbdException {
        final DataType dataType;
        dataType = getDataType(indexBasedZero);
        if (dataType instanceof PgType) {
            return ((PgType) dataType).firstJavaType();
        }
        return String.class;
    }

    @Override
    public Class<?> getSecondJavaType(int indexBasedZero) throws JdbdException {
        final DataType dataType;
        dataType = getDataType(indexBasedZero);
        if (dataType instanceof PgType) {
            return ((PgType) dataType).secondJavaType();
        }
        return null;
    }

    @Override
    public NullMode getNullMode(int indexBasedZero) throws JdbdException {
        return NullMode.UNKNOWN;
    }


    @Override
    public int getPrecision(final int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].getPrecision();
    }

    @Override
    public int getScale(final int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].getScale();
    }

    @Override
    public BooleanMode getAutoIncrementMode(int indexBasedZero) throws JdbdException {
        return BooleanMode.UNKNOWN;
    }


    @Override
    public String getCatalogName(int indexBasedZero) throws JdbdException {
        //always null
        return null;
    }


    @Override
    public String getSchemaName(int indexBasedZero) throws JdbdException {
        //always null
        return null;
    }

    @Nullable
    @Override
    public String getTableName(int indexBasedZero) throws JdbdException {
        //always null
        return null;
    }

    @Nullable
    @Override
    public String getColumnName(int indexBasedZero) throws JdbdException {
        //always null
        return null;
    }

    private int checkIndex(final int indexBasedZero) {
        if (indexBasedZero < 0 || indexBasedZero >= this.columnMetaArray.length) {
            String m = String.format("Invalid column index[%s] ,should be [0,%s).",
                    indexBasedZero, this.columnMetaArray.length);
            throw new JdbdException(m);
        }
        return indexBasedZero;
    }

    /*################################## blow packet method ##################################*/

    /*################################## blow private static method ##################################*/

    private static JdbdException createNotFoundIndexException(final String columnLabel) {
        String m = String.format("Not found column index for column label[%s]", columnLabel);
        return new JdbdException(m);
    }

    /**
     * @return a unmodifiable map
     */
    private static Map<String, Integer> createLabelToIndexMap(final PgColumnMeta[] columnMetaArray) {
        final Map<String, Integer> map;
        if (columnMetaArray.length == 1) {
            map = Collections.singletonMap(columnMetaArray[0].columnLabel, 0);
        } else {
            Map<String, Integer> tempMap = PgCollections.hashMap((int) (columnMetaArray.length / 0.75f));
            for (int i = 0; i < columnMetaArray.length; i++) {
                tempMap.put(columnMetaArray[i].columnLabel, i); // override , if duplication
            }
            map = Collections.unmodifiableMap(tempMap);
        }
        return map;
    }

    @Nullable
    private static DecimalFormat createMoneyFormatIfNeed(final PgColumnMeta[] columnMetaArray,
                                                         final TaskAdjutant adjutant) {
        DecimalFormat format = null;
        for (PgColumnMeta meta : columnMetaArray) {
            if (meta.dataType != PgType.MONEY && meta.dataType != PgType.MONEY_ARRAY) {
                continue;
            }
            format = PgNumbers.getMoneyFormat(adjutant.server().moneyLocal());
            break;
        }
        return format;
    }


}
