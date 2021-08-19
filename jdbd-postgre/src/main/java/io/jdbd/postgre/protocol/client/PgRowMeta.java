package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.meta.NullMode;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgType;
import io.jdbd.result.FieldType;
import io.jdbd.result.ResultRowMeta;
import io.netty.buffer.ByteBuf;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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

    private PgRowMeta(int resultIndex, PgColumnMeta[] columnMetaArray) {
        if (resultIndex < 0) {
            throw new IllegalArgumentException(String.format("resultIndex[%s] less than 0 .", resultIndex));
        }
        this.resultIndex = resultIndex;
        this.columnMetaArray = columnMetaArray;
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
    public List<String> getColumnAliasList() {
        final PgColumnMeta[] columnMetaArray = this.columnMetaArray;

        final List<String> labelList;
        if (columnMetaArray.length == 1) {
            labelList = Collections.singletonList(columnMetaArray[0].columnAlias);
        } else {
            List<String> list = new ArrayList<>(columnMetaArray.length);
            for (PgColumnMeta meta : columnMetaArray) {
                list.add(meta.columnAlias);
            }
            labelList = Collections.unmodifiableList(list);
        }
        return labelList;
    }

    @Override
    public final String getColumnLabel(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].columnAlias;
    }

    @Override
    public final int getColumnIndex(final String columnLabel) throws JdbdSQLException {
        Objects.requireNonNull(columnLabel, "columnLabel");
        int indexBaseZero = -1;
        final PgColumnMeta[] columnMetaArray = this.columnMetaArray;
        for (int i = 0; i < columnMetaArray.length; i++) {
            if (columnLabel.equals(columnMetaArray[i].columnAlias)) {
                indexBaseZero = i;
                break;
            }
        }
        if (indexBaseZero < 0) {
            String m = String.format("Not found column index for column label[%s]", columnLabel);
            throw new JdbdSQLException(new SQLException(m));
        }
        return indexBaseZero;
    }

    @Override
    public JDBCType getJdbdType(int indexBaseZero) throws JdbdSQLException {
        return getSQLType(indexBaseZero).jdbcType();
    }

    @Override
    public boolean isPhysicalColumn(int indexBaseZero) throws JdbdSQLException {
        return false;
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
    public NullMode getNullMode(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public NullMode getNullMode(String columnAlias) throws JdbdSQLException {
        return null;
    }

    @Override
    public boolean isUnsigned(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isUnsigned(String columnAlias) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isAutoIncrement(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isAutoIncrement(String columnAlias) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public String getCatalogName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public String getSchemaName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public String getTableName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public String getColumnName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public boolean isReadOnly(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isWritable(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public Class<?> getColumnClass(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public long getPrecision(int indexBaseZero) throws JdbdSQLException {
        return 0;
    }

    @Override
    public long getPrecision(String columnAlias) throws JdbdSQLException {
        return 0;
    }

    @Override
    public int getScale(int indexBaseZero) throws JdbdSQLException {
        return 0;
    }

    @Override
    public int getScale(String columnAlias) throws JdbdSQLException {
        return 0;
    }

    @Override
    public boolean isPrimaryKey(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isPrimaryKey(String columnAlias) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isUniqueKey(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isUniqueKey(String columnAlias) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isMultipleKey(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isMultipleKey(String columnAlias) throws JdbdSQLException {
        return false;
    }

    private int checkIndex(final int indexBasedZero) {
        if (indexBasedZero < 0 || indexBasedZero >= this.columnMetaArray.length) {
            String m = String.format("Invalid column index[%s] ,should be [0,%s)."
                    , indexBasedZero, this.columnMetaArray.length);
            throw new JdbdSQLException(new SQLException(m));
        }
        return indexBasedZero;
    }


}
