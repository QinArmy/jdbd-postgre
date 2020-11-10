package io.jdbd.mysql.protocol.client;

import io.jdbd.NullMode;
import io.jdbd.ReactiveSQLException;
import io.jdbd.ResultRowMeta;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.util.MySQLStringUtils;
import org.qinarmy.util.StringUtils;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Map;

/**
 * This class is a implementation of {@link ResultRowMeta}
 */
abstract class MySQLRowMeta implements ResultRowMeta {

    static MySQLRowMeta from(MySQLColumnMeta[] mySQLColumnMetas, Map<Integer, Integer> customIndexMblenMap) {
        return new SimpleIndexMySQLRowMeta(mySQLColumnMetas, customIndexMblenMap);
    }

    private final MySQLColumnMeta[] columnMetas;

    private final Map<Integer, Integer> customIndexMblenMap;

    private MySQLRowMeta(MySQLColumnMeta[] columnMetas, Map<Integer, Integer> customIndexMblenMap) {
        this.columnMetas = columnMetas;
        this.customIndexMblenMap = customIndexMblenMap;
    }

    @Override
    public final int getColumnCount() {
        return this.columnMetas.length;
    }

    @Override
    public final JDBCType getJdbdType(int indexBaseOne) throws ReactiveSQLException {
        return this.columnMetas[convertIndex(indexBaseOne)].mysqlType.jdbcType();
    }

    @Override
    public final JDBCType getJdbdType(String columnLabel) throws ReactiveSQLException {
        return getJdbdType(convertToIndex(columnLabel));
    }

    @Override
    public final boolean isPhysicalColumn(int indexBaseOne) throws ReactiveSQLException {
        MySQLColumnMeta columnMeta = this.columnMetas[convertIndex(indexBaseOne)];
        return StringUtils.hasText(columnMeta.tableName)
                && StringUtils.hasText(columnMeta.columnName);
    }

    @Override
    public final boolean isPhysicalColumn(String columnLabel) throws ReactiveSQLException {
        return isPhysicalColumn(convertToIndex(columnLabel));
    }

    @Override
    public final SQLType getSQLType(int indexBaseOne) throws ReactiveSQLException {
        return this.columnMetas[convertIndex(indexBaseOne)].mysqlType;
    }

    @Override
    public final SQLType getSQLType(String columnLabel) throws ReactiveSQLException {
        return getSQLType(convertToIndex(columnLabel));
    }

    @Override
    public final NullMode getNullMode(int indexBaseOne) throws ReactiveSQLException {
        return (this.columnMetas[convertIndex(indexBaseOne)].definitionFlags & MySQLColumnMeta.NOT_NULL_FLAG) != 0
                ? NullMode.NON_NULL
                : NullMode.NULLABLE;
    }

    @Override
    public final NullMode getNullMode(String columnLabel) throws ReactiveSQLException {
        return getNullMode(convertToIndex(columnLabel));
    }

    @Override
    public final boolean isSigned(int indexBaseOne) throws ReactiveSQLException {
        return (this.columnMetas[convertIndex(indexBaseOne)].definitionFlags & MySQLColumnMeta.UNSIGNED_FLAG) == 0;
    }

    @Override
    public final boolean isSigned(String columnLabel) throws ReactiveSQLException {
        return isSigned(convertToIndex(columnLabel));
    }

    @Override
    public final boolean isAutoIncrement(int indexBaseOne) throws ReactiveSQLException {
        return (this.columnMetas[convertIndex(indexBaseOne)].definitionFlags & MySQLColumnMeta.AUTO_INCREMENT_FLAG) != 0;
    }

    @Override
    public final boolean isAutoIncrement(String columnLabel) throws ReactiveSQLException {
        return isAutoIncrement(convertToIndex(columnLabel));
    }

    @Override
    public final boolean isCaseSensitive(int indexBaseOne) throws ReactiveSQLException {
        return doIsCaseSensitive(this.columnMetas[convertIndex(indexBaseOne)]);
    }

    @Override
    public final boolean isCaseSensitive(String columnLabel) throws ReactiveSQLException {
        return isCaseSensitive(convertToIndex(columnLabel));
    }

    @Nullable
    @Override
    public final String getCatalogName(int indexBaseOne) throws ReactiveSQLException {
        return this.columnMetas[convertIndex(indexBaseOne)].catalogName;
    }

    @Nullable
    @Override
    public final String getCatalogName(String columnLabel) throws ReactiveSQLException {
        return getCatalogName(convertToIndex(columnLabel));
    }

    @Nullable
    @Override
    public final String getSchemaName(int indexBaseOne) throws ReactiveSQLException {
        return this.columnMetas[convertIndex(indexBaseOne)].schemaName;
    }

    @Nullable
    @Override
    public final String getSchemaName(String columnLabel) throws ReactiveSQLException {
        return getSchemaName(convertToIndex(columnLabel));
    }

    @Nullable
    @Override
    public final String getTableName(int indexBaseOne) throws ReactiveSQLException {
        return this.columnMetas[convertIndex(indexBaseOne)].tableName;
    }

    @Nullable
    @Override
    public final String getTableName(String columnLabel) throws ReactiveSQLException {
        return getTableName(convertToIndex(columnLabel));
    }

    @Override
    public final String getColumnLabel(int indexBaseOne) throws ReactiveSQLException {
        return this.columnMetas[convertIndex(indexBaseOne)].columnAlias;
    }

    @Nullable
    @Override
    public final String getColumnName(int indexBaseOne) throws ReactiveSQLException {
        return this.columnMetas[convertIndex(indexBaseOne)].columnName;
    }

    @Override
    public boolean isReadOnly(int indexBaseOne) throws ReactiveSQLException {
        MySQLColumnMeta columnMeta = this.columnMetas[convertIndex(indexBaseOne)];
        return MySQLStringUtils.isEmpty(columnMeta.tableName)
                && MySQLStringUtils.isEmpty(columnMeta.columnName);
    }

    @Override
    public final boolean isReadOnly(String columnLabel) throws ReactiveSQLException {
        return isReadOnly(convertToIndex(columnLabel));
    }

    @Override
    public final boolean isWritable(int indexBaseOne) throws ReactiveSQLException {
        return !isReadOnly(indexBaseOne);
    }

    @Override
    public final boolean isWritable(String columnLabel) throws ReactiveSQLException {
        return isWritable(convertToIndex(columnLabel));
    }

    @Override
    public final Class<?> getColumnClass(int indexBaseOne) throws ReactiveSQLException {
        return this.columnMetas[convertIndex(indexBaseOne)].mysqlType.javaType();
    }

    @Override
    public final Class<?> getColumnClass(String columnLabel) throws ReactiveSQLException {
        return getColumnClass(convertToIndex(columnLabel));
    }

    @Override
    public final long getPrecision(int indexBaseOne) throws ReactiveSQLException {
        return obtainPrecision(this.columnMetas[convertIndex(indexBaseOne)]);
    }

    @Override
    public final long getPrecision(String columnLabel) throws ReactiveSQLException {
        return getPrecision(convertToIndex(columnLabel));
    }

    @Override
    public int getScale(int indexBaseOne) throws ReactiveSQLException {
        MySQLColumnMeta columnMeta = this.columnMetas[convertIndex(indexBaseOne)];
        return (columnMeta.mysqlType == MySQLType.DECIMAL || columnMeta.mysqlType == MySQLType.DECIMAL_UNSIGNED)
                ? columnMeta.decimals
                : 0;
    }

    @Override
    public int getScale(String columnLabel) throws ReactiveSQLException {
        return getScale(convertToIndex(columnLabel));
    }


    @Override
    public boolean isPrimaryKey(int indexBaseOne) throws ReactiveSQLException {
        return (this.columnMetas[convertIndex(indexBaseOne)].definitionFlags & MySQLColumnMeta.PRI_KEY_FLAG) != 0;
    }

    @Override
    public boolean isPrimaryKey(String columnLabel) throws ReactiveSQLException {
        return isPrimaryKey(convertToIndex(columnLabel));
    }

    @Override
    public boolean isUniqueKey(int indexBaseOne) throws ReactiveSQLException {
        return (this.columnMetas[convertIndex(indexBaseOne)].definitionFlags & MySQLColumnMeta.UNIQUE_KEY_FLAG) != 0;
    }

    @Override
    public boolean isUniqueKey(String columnLabel) throws ReactiveSQLException {
        return isUniqueKey(convertToIndex(columnLabel));
    }

    @Override
    public boolean isMultipleKey(int indexBaseOne) throws ReactiveSQLException {
        return (this.columnMetas[convertIndex(indexBaseOne)].definitionFlags & MySQLColumnMeta.MULTIPLE_KEY_FLAG) != 0;
    }

    @Override
    public boolean isMultipleKey(String columnLabel) throws ReactiveSQLException {
        return isMultipleKey(convertToIndex(columnLabel));
    }

    int convertToIndex(String columnLabel) {
        MySQLColumnMeta[] columnMetas = this.columnMetas;
        int len = columnMetas.length;
        for (int i = 0; i < len; i++) {
            if (columnMetas[i].columnAlias.equals(columnLabel)) {
                return i;
            }
        }
        throw new ReactiveSQLException(
                new SQLException(String.format("not found index for columnLabel[%s]", columnLabel)));
    }

    private int convertIndex(int indexBaseOne) {
        if (indexBaseOne < 1 || indexBaseOne > this.columnMetas.length) {
            throw new ReactiveSQLException(new SQLException(
                    String.format("index[%s] out of bounds[1 -- %s].", indexBaseOne, columnMetas.length)));
        }
        return indexBaseOne - 1;
    }

    private boolean doIsCaseSensitive(MySQLColumnMeta columnMeta) {
        boolean caseSensitive;
        switch (columnMeta.mysqlType) {
            case BIT:
            case TINYINT:
            case SMALLINT:
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
                caseSensitive = false;
                break;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case ENUM:
            case SET:
                String collationName = CharsetMapping.getCollationNameByIndex(columnMeta.collationIndex);
                caseSensitive = ((collationName != null) && !collationName.endsWith("_ci"));
                break;
            default:
                caseSensitive = true;
        }
        return caseSensitive;
    }

    long obtainPrecision(MySQLColumnMeta columnMeta) {
        long precision;
        // Protocol returns precision and scale differently for some types. We need to align then to I_S.
        switch (columnMeta.mysqlType) {
            case DECIMAL:
                precision = columnMeta.length;
                precision--;
                if (columnMeta.decimals > 0) {
                    precision--;
                }
                break;
            case DECIMAL_UNSIGNED:
                precision = columnMeta.length;
                if (columnMeta.decimals > 0) {
                    precision--;
                }
                break;
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                precision = columnMeta.length;
                break;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                // char
                int collationIndex = columnMeta.collationIndex;
                Integer mblen = this.customIndexMblenMap.get(collationIndex);
                if (mblen == null) {
                    mblen = CharsetMapping.getMblen(collationIndex);
                }
                precision = columnMeta.length / mblen;
                break;
            case YEAR:
            case DATE:
                precision = 0L;
                break;
            case TIME:
                precision = columnMeta.length - 11L;
                if (precision < 0) {
                    precision = 0;
                }
                break;
            case TIMESTAMP:
            case DATETIME:
                precision = columnMeta.length - 20L;
                if (precision < 0) {
                    precision = 0;
                }
                break;
            default:
                precision = -1;

        }
        return precision;
    }


    private static final class SimpleIndexMySQLRowMeta extends MySQLRowMeta {

        private SimpleIndexMySQLRowMeta(MySQLColumnMeta[] columnMetas
                , Map<Integer, Integer> customIndexMblenMap) {
            super(columnMetas, customIndexMblenMap);
        }
    }

}
