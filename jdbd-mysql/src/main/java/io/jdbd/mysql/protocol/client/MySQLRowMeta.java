package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.NullMode;
import io.jdbd.ResultRowMeta;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.util.MySQLStringUtils;
import org.qinarmy.util.StringUtils;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This class is a implementation of {@link ResultRowMeta}
 */
abstract class MySQLRowMeta implements ResultRowMeta {

    static final MySQLRowMeta EMPTY = from(MySQLColumnMeta.EMPTY, Collections.emptyMap());

    static MySQLRowMeta from(MySQLColumnMeta[] mySQLColumnMetas
            , Map<Integer, CharsetMapping.CustomCollation> customCollationMap) {
        return new SimpleIndexMySQLRowMeta(mySQLColumnMetas, customCollationMap);
    }

    final MySQLColumnMeta[] columnMetaArray;

    final Map<Integer, CharsetMapping.CustomCollation> customCollationMap;

    int metaIndex = 0;


    private MySQLRowMeta(final MySQLColumnMeta[] columnMetaArray
            , Map<Integer, CharsetMapping.CustomCollation> customCollationMap) {
        this.columnMetaArray = columnMetaArray;
        this.customCollationMap = customCollationMap;

    }

    @Override
    public final int getColumnCount() {
        return this.columnMetaArray.length;
    }

    @Override
    public List<String> getColumnAliasList() {
        List<String> columnAliaList;
        if (this.columnMetaArray.length == 1) {
            columnAliaList = Collections.singletonList(this.columnMetaArray[0].columnAlias);
        } else {
            columnAliaList = new ArrayList<>(this.columnMetaArray.length);
            for (MySQLColumnMeta columnMeta : this.columnMetaArray) {
                columnAliaList.add(columnMeta.columnAlias);
            }
            columnAliaList = Collections.unmodifiableList(columnAliaList);
        }
        return columnAliaList;
    }

    @Override
    public final JDBCType getJdbdType(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].mysqlType.jdbcType();
    }


    @Override
    public final boolean isPhysicalColumn(int indexBaseZero) throws JdbdSQLException {
        MySQLColumnMeta columnMeta = this.columnMetaArray[checkIndex(indexBaseZero)];
        return StringUtils.hasText(columnMeta.tableName)
                && StringUtils.hasText(columnMeta.columnName);
    }


    @Override
    public final SQLType getSQLType(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].mysqlType;
    }


    @Override
    public final NullMode getNullMode(int indexBaseZero) throws JdbdSQLException {
        return (this.columnMetaArray[checkIndex(indexBaseZero)].definitionFlags & MySQLColumnMeta.NOT_NULL_FLAG) != 0
                ? NullMode.NON_NULL
                : NullMode.NULLABLE;
    }


    @Override
    public final boolean isSigned(int indexBaseZero) throws JdbdSQLException {
        return (this.columnMetaArray[checkIndex(indexBaseZero)].definitionFlags & MySQLColumnMeta.UNSIGNED_FLAG) == 0;
    }


    @Override
    public final boolean isAutoIncrement(int indexBaseZero) throws JdbdSQLException {
        return (this.columnMetaArray[checkIndex(indexBaseZero)].definitionFlags & MySQLColumnMeta.AUTO_INCREMENT_FLAG) != 0;
    }


    @Override
    public final boolean isCaseSensitive(int indexBaseZero) throws JdbdSQLException {
        return doIsCaseSensitive(this.columnMetaArray[checkIndex(indexBaseZero)]);
    }


    @Nullable
    @Override
    public final String getCatalogName(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].catalogName;
    }


    @Nullable
    @Override
    public final String getSchemaName(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].schemaName;
    }


    @Nullable
    @Override
    public final String getTableName(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].tableName;
    }


    @Override
    public final String getColumnLabel(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].columnAlias;
    }

    @Override
    public int getColumnIndex(String columnLabel) throws JdbdSQLException {
        return convertToIndex(columnLabel);
    }

    @Nullable
    @Override
    public final String getColumnName(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].columnName;
    }

    @Override
    public boolean isReadOnly(int indexBaseZero) throws JdbdSQLException {
        MySQLColumnMeta columnMeta = this.columnMetaArray[checkIndex(indexBaseZero)];
        return MySQLStringUtils.isEmpty(columnMeta.tableName)
                && MySQLStringUtils.isEmpty(columnMeta.columnName);
    }


    @Override
    public final boolean isWritable(int indexBaseZero) throws JdbdSQLException {
        return !isReadOnly(indexBaseZero);
    }


    @Override
    public final Class<?> getColumnClass(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)].mysqlType.javaType();
    }


    @Override
    public final long getPrecision(int indexBaseZero) throws JdbdSQLException {
        return this.columnMetaArray[checkIndex(indexBaseZero)]
                .obtainPrecision(this.customCollationMap);
    }


    @Override
    public int getScale(int indexBaseZero) throws JdbdSQLException {
        MySQLColumnMeta columnMeta = this.columnMetaArray[checkIndex(indexBaseZero)];
        return (columnMeta.mysqlType == MySQLType.DECIMAL || columnMeta.mysqlType == MySQLType.DECIMAL_UNSIGNED)
                ? columnMeta.decimals
                : 0;
    }


    @Override
    public boolean isPrimaryKey(int indexBaseZero) throws JdbdSQLException {
        return (this.columnMetaArray[checkIndex(indexBaseZero)].definitionFlags & MySQLColumnMeta.PRI_KEY_FLAG) != 0;
    }


    @Override
    public boolean isUniqueKey(int indexBaseZero) throws JdbdSQLException {
        return (this.columnMetaArray[checkIndex(indexBaseZero)].definitionFlags & MySQLColumnMeta.UNIQUE_KEY_FLAG) != 0;
    }

    @Override
    public boolean isMultipleKey(int indexBaseZero) throws JdbdSQLException {
        return (this.columnMetaArray[checkIndex(indexBaseZero)].definitionFlags & MySQLColumnMeta.MULTIPLE_KEY_FLAG) != 0;
    }

    boolean isReady() {
        return this.metaIndex == this.columnMetaArray.length;
    }

    int convertToIndex(String columnLabel) {
        MySQLColumnMeta[] columnMetas = this.columnMetaArray;
        int len = columnMetas.length;
        for (int i = 0; i < len; i++) {
            if (columnMetas[i].columnAlias.equals(columnLabel)) {
                return i;
            }
        }
        throw new JdbdSQLException(
                new SQLException(String.format("not found index for columnLabel[%s]", columnLabel)));
    }

    final MySQLType getMySQLType(int indexBaseZero) {
        return this.columnMetaArray[checkIndex(indexBaseZero)].mysqlType;
    }

    public final Charset getColumnCharset(int indexBaseZero) {
        return this.columnMetaArray[checkIndex(indexBaseZero)].columnCharset;
    }

    private int checkIndex(int indexBaseZero) {
        if (indexBaseZero < 0 || indexBaseZero >= this.columnMetaArray.length) {
            throw new JdbdSQLException(new SQLException(
                    String.format("index[%s] out of bounds[0 -- %s].", indexBaseZero, columnMetaArray.length - 1)));
        }
        return indexBaseZero;
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


    private static final class SimpleIndexMySQLRowMeta extends MySQLRowMeta {

        private SimpleIndexMySQLRowMeta(MySQLColumnMeta[] columnMetas
                , Map<Integer, CharsetMapping.CustomCollation> customCollationMap) {
            super(columnMetas, customCollationMap);
        }
    }


}
