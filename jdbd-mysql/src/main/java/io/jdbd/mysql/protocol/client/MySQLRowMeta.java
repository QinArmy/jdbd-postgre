package io.jdbd.mysql.protocol.client;

import com.mysql.cj.CharsetMapping;
import io.jdbd.NullMode;
import io.jdbd.ReactiveSQLException;
import io.jdbd.ResultRowMeta;
import io.jdbd.meta.SQLType;
import org.qinarmy.util.StringUtils;

import java.sql.JDBCType;
import java.sql.SQLException;

abstract class MySQLRowMeta implements ResultRowMeta {

    static MySQLRowMeta from(MySQLColumnMeta[] mySQLColumnMetas) {
        return null;
    }

    private final MySQLColumnMeta[] columnMetas;

    private MySQLRowMeta(MySQLColumnMeta[] columnMetas) {
        this.columnMetas = columnMetas;
    }

    @Override
    public final int getColumnCount() {
        return this.columnMetas.length;
    }

    @Override
    public final JDBCType getJdbdType(int index) throws ReactiveSQLException {
        checkIndex(index);
        return this.columnMetas[index].mysqlType.jdbcType();
    }

    @Override
    public final JDBCType getJdbdType(String columnLabel) throws ReactiveSQLException {
        return getJdbdType(convertToIndex(columnLabel));
    }

    @Override
    public final boolean isPhysicalColumn(int index) throws ReactiveSQLException {
        checkIndex(index);
        MySQLColumnMeta columnMeta = this.columnMetas[index];
        return StringUtils.hasText(columnMeta.tableName)
                && StringUtils.hasText(columnMeta.columnName);
    }

    @Override
    public final boolean isPhysicalColumn(String columnLabel) throws ReactiveSQLException {
        return isPhysicalColumn(convertToIndex(columnLabel));
    }

    @Override
    public final SQLType getSQLType(int index) throws ReactiveSQLException {
        checkIndex(index);
        return this.columnMetas[index].mysqlType;
    }

    @Override
    public final SQLType getSQLType(String columnLabel) throws ReactiveSQLException {
        return getSQLType(convertToIndex(columnLabel));
    }

    @Override
    public final NullMode getNullMode(int index) throws ReactiveSQLException {
        checkIndex(index);
        return (this.columnMetas[index].definitionFlags & MySQLColumnMeta.NOT_NULL_FLAG) != 0
                ? NullMode.NON_NULL
                : NullMode.NULLABLE;
    }

    @Override
    public final NullMode getNullMode(String columnLabel) throws ReactiveSQLException {
        return getNullMode(convertToIndex(columnLabel));
    }

    @Override
    public final boolean isSigned(int index) throws ReactiveSQLException {
        checkIndex(index);
        return (this.columnMetas[index].definitionFlags & MySQLColumnMeta.UNSIGNED_FLAG) == 0;
    }

    @Override
    public final boolean isSigned(String columnLabel) throws ReactiveSQLException {
        return isSigned(convertToIndex(columnLabel));
    }

    @Override
    public final boolean isAutoIncrement(int index) throws ReactiveSQLException {
        checkIndex(index);
        return (this.columnMetas[index].definitionFlags & MySQLColumnMeta.AUTO_INCREMENT_FLAG) != 0;
    }

    @Override
    public final boolean isAutoIncrement(String columnLabel) throws ReactiveSQLException {
        return isAutoIncrement(convertToIndex(columnLabel));
    }

    @Override
    public boolean isCaseSensitive(int index) throws ReactiveSQLException {
        checkIndex(index);
        return false;
    }

    @Override
    public boolean isCaseSensitive(String columnLabel) throws ReactiveSQLException {
        return isCaseSensitive(convertToIndex(columnLabel));
    }

    @Override
    public String getCatalogName(int index) throws ReactiveSQLException {
        return null;
    }

    @Override
    public String getCatalogName(String columnLabel) throws ReactiveSQLException {
        return null;
    }

    @Override
    public String getSchemaName(int index) throws ReactiveSQLException {
        return null;
    }

    @Override
    public String getSchemaName(String columnLabel) throws ReactiveSQLException {
        return null;
    }

    @Override
    public String getTableName(int index) throws ReactiveSQLException {
        return null;
    }

    @Override
    public String getTableName(String columnLabel) throws ReactiveSQLException {
        return null;
    }

    @Override
    public String getColumnLabel(int index) throws ReactiveSQLException {
        return null;
    }

    @Override
    public String getColumnName(int index) throws ReactiveSQLException {
        return null;
    }

    @Override
    public boolean isReadOnly(int index) throws ReactiveSQLException {
        return false;
    }

    @Override
    public boolean isReadOnly(String columnLabel) throws ReactiveSQLException {
        return false;
    }

    @Override
    public boolean isWritable(int index) throws ReactiveSQLException {
        return false;
    }

    @Override
    public boolean isWritable(String columnLabel) throws ReactiveSQLException {
        return false;
    }

    @Override
    public Class<?> getColumnClass(int index) throws ReactiveSQLException {
        return null;
    }

    @Override
    public Class<?> getColumnClass(String columnLabel) throws ReactiveSQLException {
        return null;
    }

    @Override
    public long getPrecision(int index) throws ReactiveSQLException {
        return 0;
    }

    @Override
    public long getPrecision(String columnLabel) throws ReactiveSQLException {
        return 0;
    }

    @Override
    public int getScale(int index) throws ReactiveSQLException {
        return 0;
    }

    @Override
    public int getScale(String columnLabel) throws ReactiveSQLException {
        return 0;
    }

    @Override
    public boolean isPrimaryKey(int index) throws ReactiveSQLException {
        return false;
    }

    @Override
    public boolean isPrimaryKey(String columnLabel) throws ReactiveSQLException {
        return false;
    }

    @Override
    public boolean isUniqueKey(int index) throws ReactiveSQLException {
        return false;
    }

    @Override
    public boolean isUniqueKey(String columnLabel) throws ReactiveSQLException {
        return false;
    }

    @Override
    public boolean isMultipleKey(int index) throws ReactiveSQLException {
        return false;
    }

    @Override
    public boolean isMultipleKey(String columnLabel) throws ReactiveSQLException {
        return false;
    }

    int convertToIndex(String columnLabel) {
        MySQLColumnMeta[] columnMetas = this.columnMetas;
        int len = columnMetas.length;
        for (int i = 0; i < len; i++) {
            if (columnMetas[i].alias.equals(columnLabel)) {
                return i;
            }
        }
        throw new ReactiveSQLException(
                new SQLException(String.format("not found index for columnLabel[%s]", columnLabel)));
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= this.columnMetas.length) {
            throw new ReactiveSQLException(new SQLException(
                    String.format("index[%s] out of bounds[%s].", index, columnMetas.length)))
        }
    }

    private boolean doIsCaseSensitive(MySQLColumnMeta columnMeta) {
        switch (columnMeta.mysqlType) {
            case BIT:
            case TINY:
            case SHORT:
            case INT:
            case INT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case BIGINT:
            case BIGINT_UNSIGNED:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DATE:
            case YEAR:
            case TIME:
            case TIMESTAMP:
            case DATETIME:
                return false;

            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case ENUM:
            case SET:
                String collationName = CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[field.getCollationIndex()];
                return ((collationName != null) && !collationName.endsWith("_ci"));

            default:
                return true;
        }
    }

}
