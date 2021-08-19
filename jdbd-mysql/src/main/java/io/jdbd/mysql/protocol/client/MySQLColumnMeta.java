package io.jdbd.mysql.protocol.client;

import io.jdbd.meta.NullMode;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.result.FieldType;
import io.jdbd.vendor.conf.Properties;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Objects;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html"> Column Definition Flags</a>
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html"> Column Definition Protocol</a>
 */
final class MySQLColumnMeta {

    static final MySQLColumnMeta[] EMPTY = new MySQLColumnMeta[0];

    private static final Logger LOG = LoggerFactory.getLogger(MySQLColumnMeta.class);

    static final int NOT_NULL_FLAG = 1;
    static final int PRI_KEY_FLAG = 1 << 1;
    static final int UNIQUE_KEY_FLAG = 1 << 4;
    static final int MULTIPLE_KEY_FLAG = 1 << 3;

    static final int BLOB_FLAG = 1 << 4;
    static final int UNSIGNED_FLAG = 1 << 5;
    static final int ZEROFILL_FLAG = 1 << 6;
    static final int BINARY_FLAG = 1 << 7;

    static final int ENUM_FLAG = 1 << 8;
    static final int AUTO_INCREMENT_FLAG = 1 << 9;
    static final int TIMESTAMP_FLAG = 1 << 10;
    static final int SET_FLAG = 1 << 11;

    static final int NO_DEFAULT_VALUE_FLAG = 1 << 12;
    static final int ON_UPDATE_NOW_FLAG = 1 << 13;
    static final int PART_KEY_FLAG = 1 << 14;
    static final int NUM_FLAG = 1 << 15;

    final String catalogName;

    final String schemaName;

    final String tableName;

    final String tableAlias;

    final String columnName;

    final String columnAlias;

    final int collationIndex;

    final Charset columnCharset;

    final long fixedLength;

    final long length;

    final int typeFlag;

    final int definitionFlags;

    final short decimals;

    final FieldType fieldType;

    final MySQLType mysqlType;

    private MySQLColumnMeta(
            @Nullable String catalogName, @Nullable String schemaName
            , @Nullable String tableName, @Nullable String tableAlias
            , @Nullable String columnName, String columnAlias
            , int collationIndex, Charset columnCharset
            , long fixedLength, long length
            , int typeFlag, int definitionFlags
            , short decimals, Properties<PropertyKey> properties) {

        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableAlias = tableAlias;

        this.columnName = columnName;
        this.columnAlias = columnAlias;
        this.collationIndex = collationIndex;
        this.columnCharset = columnCharset;

        this.fixedLength = fixedLength;
        this.length = length;
        this.typeFlag = typeFlag;
        this.definitionFlags = definitionFlags;

        this.decimals = decimals;
        this.fieldType = parseFieldType(this);
        // mysqlType must be last
        this.mysqlType = from(this, properties);
    }

    public boolean isUnsigned() {
        return (this.definitionFlags & UNSIGNED_FLAG) != 0;
    }


    public long obtainPrecision(Map<Integer, CharsetMapping.CustomCollation> customCollationMap) {
        long precision;
        // Protocol returns precision and scale differently for some types. We need to align then to I_S.
        switch (this.mysqlType) {
            case DECIMAL:
                precision = this.length;
                precision--;// signed
                if (this.decimals > 0) {
                    precision--; // point
                }
                break;
            case DECIMAL_UNSIGNED:
                precision = this.length;
                if (this.decimals > 0) {
                    precision--;// point
                }
                break;
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case BIT:
                precision = this.length;
                break;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                // char
                int collationIndex = this.collationIndex;
                CharsetMapping.CustomCollation collation = customCollationMap.get(collationIndex);
                Integer mblen = null;
                if (collation != null) {
                    mblen = collation.maxLen;
                }
                if (mblen == null) {
                    mblen = CharsetMapping.getMblen(collationIndex);
                }
                precision = this.length / mblen;
                break;
            case TIME:
                precision = obtainTimeTypePrecision();
                break;
            case TIMESTAMP:
            case DATETIME:
                precision = obtainDateTimeTypePrecision();
                break;
            default:
                precision = -1;

        }
        return precision;
    }

    final boolean isEnum() {
        return (this.definitionFlags & ENUM_FLAG) != 0;
    }

    final boolean isSetType() {
        return (this.definitionFlags & SET_FLAG) != 0;
    }

    final boolean isBinary() {
        return (this.definitionFlags & BINARY_FLAG) != 0;
    }

    final boolean isBlob() {
        return (this.definitionFlags & BLOB_FLAG) != 0;
    }

    final boolean isAutoIncrement() {
        return (this.definitionFlags & AUTO_INCREMENT_FLAG) != 0;
    }

    final boolean isPrimaryKey() {
        return (this.definitionFlags & PRI_KEY_FLAG) != 0;
    }

    final boolean isMultipleKey() {
        return (this.definitionFlags & MULTIPLE_KEY_FLAG) != 0;
    }

    final boolean isUniqueKey() {
        return (this.definitionFlags & UNIQUE_KEY_FLAG) != 0;
    }

    final int getScale() {
        final int scale;
        switch (mysqlType) {
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                scale = this.decimals;
                break;
            default:
                scale = -1;
        }
        return scale;
    }

    final NullMode getNullMode() {
        final NullMode nullMode;
        if ((this.definitionFlags & NOT_NULL_FLAG) != 0 || (this.definitionFlags & PRI_KEY_FLAG) != 0) {
            nullMode = NullMode.NON_NULL;
        } else {
            nullMode = NullMode.NULLABLE;
        }
        return nullMode;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MySQLColumnMeta{");
        sb.append("catalogName='").append(catalogName).append('\'');
        sb.append(",\n schemaName='").append(schemaName).append('\'');
        sb.append(",\n tableName='").append(tableName).append('\'');
        sb.append(",\n tableAlias='").append(tableAlias).append('\'');
        sb.append(",\n columnName='").append(columnName).append('\'');
        sb.append(",\n columnAlias='").append(columnAlias).append('\'');
        sb.append(",\n collationIndex=").append(collationIndex);
        sb.append(",\n fixedLength=").append(fixedLength);
        sb.append(",\n length=").append(length);
        sb.append(",\n typeFlag=").append(typeFlag);
        sb.append(",\n definitionFlags=").append(definitionFlags);
        sb.append(",\n decimals=").append(decimals);
        sb.append(",\n mysqlType=").append(mysqlType);
        sb.append('}');
        return sb.toString();
    }

    int obtainTimeTypePrecision() {
        final int precision;
        if (this.decimals > 0 && this.decimals < 7) {
            precision = this.decimals;
        } else if (this.length == 10) {
            precision = 0;
        } else {
            precision = (int) (this.length - 11L);
            if (precision < 0 || precision > 6) {
                throw new IllegalArgumentException(String.format("MySQLColumnMeta[%s] isn't time type.", this));
            }
        }
        return precision;
    }

    final int obtainDateTimeTypePrecision() {
        final int precision;
        if (this.decimals > 0 && this.decimals < 7) {
            precision = this.decimals;
        } else if (this.length == 19) {
            precision = 0;
        } else {
            precision = (int) (this.length - 20L);
            if (precision < 0 || precision > 6) {
                throw new IllegalArgumentException(String.format("MySQLColumnMeta[%s] isn't time type.", this));
            }
        }
        return precision;
    }

    final boolean isTiny1AsBit() {
        return this.typeFlag == ProtocolConstants.TYPE_TINY && this.length == 1 && !this.isUnsigned();
    }


    private FieldType parseFieldType(MySQLColumnMeta columnMeta) {
        final String tableName = columnMeta.tableName;
        final FieldType fieldType;
        if (MySQLStringUtils.hasText(tableName)) {
            // TODO zoro complete
            if (tableName.startsWith("#sql_")) {
                fieldType = FieldType.PHYSICAL_FILED;
            } else {
                fieldType = FieldType.FIELD;
            }
        } else {
            fieldType = FieldType.EXPRESSION;
        }
        return fieldType;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html">Protocol::ColumnDefinition41</a>
     */
    static MySQLColumnMeta readFor41(ByteBuf payloadBuf, ClientProtocolAdjutant adjutant) {
        final Charset metaCharset = adjutant.obtainCharsetMeta();
        final Properties<PropertyKey> properties = adjutant.obtainHostInfo().getProperties();
        // 1. catalog
        String catalogName = Packets.readStringLenEnc(payloadBuf, metaCharset);
        // 2. schema
        String schemaName = Packets.readStringLenEnc(payloadBuf, metaCharset);
        // 3. table,virtual table name
        String tableAlias = Packets.readStringLenEnc(payloadBuf, metaCharset);
        // 4. org_table,physical table name
        String tableName = Packets.readStringLenEnc(payloadBuf, metaCharset);

        // 5. name ,virtual column name,alias in select statement
        String columnAlias = Objects.requireNonNull(Packets.readStringLenEnc(payloadBuf, metaCharset)
                , "columnAlias");
        // 6. org_name,physical column name
        String columnName = Packets.readStringLenEnc(payloadBuf, metaCharset);
        // 7. length of fixed length fields ,[0x0c]
        //
        long fixLength = Packets.readLenEnc(payloadBuf);
        // 8. character_set of column
        int collationIndex = Packets.readInt2AsInt(payloadBuf);
        Charset columnCharset = CharsetMapping.getJavaCharsetByCollationIndex(collationIndex
                , adjutant.obtainCustomCollationMap());
        // 9. column_length,maximum length of the field
        long length = Packets.readInt4AsLong(payloadBuf);
        // 10. type,type of the column as defined in enum_field_types,type of the column as defined in enum_field_types
        int typeFlag = Packets.readInt1AsInt(payloadBuf);
        // 11. flags,Flags as defined in Column Definition Flags
        int definitionFlags = Packets.readInt2AsInt(payloadBuf);
        // 12. decimals,max shown decimal digits:
        //0x00 for integers and static strings
        //0x1f for dynamic strings, double, float
        //0x00 to 0x51 for decimals
        short decimals = (short) Packets.readInt1AsInt(payloadBuf);

        return new MySQLColumnMeta(
                catalogName, schemaName
                , tableName, tableAlias
                , columnName, columnAlias
                , collationIndex, columnCharset
                , fixLength, length
                , typeFlag, definitionFlags
                , decimals, properties
        );
    }


    private MySQLType from(MySQLColumnMeta columnMeta, Properties<PropertyKey> properties) {
        final MySQLType mySQLType;
        switch (columnMeta.typeFlag) {
            case ProtocolConstants.TYPE_DECIMAL:
            case ProtocolConstants.TYPE_NEWDECIMAL:
                mySQLType = columnMeta.isUnsigned() ? MySQLType.DECIMAL_UNSIGNED : MySQLType.DECIMAL;
                break;
            case ProtocolConstants.TYPE_TINY:
                mySQLType = fromTiny(columnMeta, properties);
                break;
            case ProtocolConstants.TYPE_LONG:
                mySQLType = columnMeta.isUnsigned() ? MySQLType.INT_UNSIGNED : MySQLType.INT;
                break;
            case ProtocolConstants.TYPE_LONGLONG:
                mySQLType = columnMeta.isUnsigned() ? MySQLType.BIGINT_UNSIGNED : MySQLType.BIGINT;
                break;
            case ProtocolConstants.TYPE_TIMESTAMP:
                mySQLType = MySQLType.TIMESTAMP;
                break;
            case ProtocolConstants.TYPE_INT24:
                mySQLType = columnMeta.isUnsigned() ? MySQLType.MEDIUMINT_UNSIGNED : MySQLType.MEDIUMINT;
                break;
            case ProtocolConstants.TYPE_DATE:
                mySQLType = MySQLType.DATE;
                break;
            case ProtocolConstants.TYPE_TIME:
                mySQLType = MySQLType.TIME;
                break;
            case ProtocolConstants.TYPE_DATETIME:
                mySQLType = MySQLType.DATETIME;
                break;
            case ProtocolConstants.TYPE_YEAR:
                mySQLType = MySQLType.YEAR;
                break;
            case ProtocolConstants.TYPE_VARCHAR:
            case ProtocolConstants.TYPE_VAR_STRING:
                mySQLType = fromVarcharOrVarString(columnMeta, properties);
                break;
            case ProtocolConstants.TYPE_STRING:
                mySQLType = fromString(columnMeta, properties);
                break;
            case ProtocolConstants.TYPE_SHORT: {
                mySQLType = columnMeta.isUnsigned() ? MySQLType.SMALLINT_UNSIGNED : MySQLType.SMALLINT;
            }
            break;
            case ProtocolConstants.TYPE_BIT:
                mySQLType = MySQLType.BIT;
                break;
            case ProtocolConstants.TYPE_JSON:
                mySQLType = MySQLType.JSON;
                break;
            case ProtocolConstants.TYPE_ENUM:
                mySQLType = MySQLType.ENUM;
                break;
            case ProtocolConstants.TYPE_SET:
                mySQLType = MySQLType.SET;
                break;
            case ProtocolConstants.TYPE_NULL:
                mySQLType = MySQLType.NULL;
                break;
            case ProtocolConstants.TYPE_FLOAT:
                mySQLType = columnMeta.isUnsigned() ? MySQLType.FLOAT_UNSIGNED : MySQLType.FLOAT;
                break;
            case ProtocolConstants.TYPE_DOUBLE:
                mySQLType = columnMeta.isUnsigned() ? MySQLType.DOUBLE_UNSIGNED : MySQLType.DOUBLE;
                break;
            case ProtocolConstants.TYPE_TINY_BLOB: {
                mySQLType = fromTinyBlob(columnMeta, properties);
            }
            break;
            case ProtocolConstants.TYPE_MEDIUM_BLOB: {
                mySQLType = fromMediumBlob(columnMeta, properties);
            }
            break;
            case ProtocolConstants.TYPE_LONG_BLOB: {
                mySQLType = fromLongBlob(columnMeta, properties);
            }
            break;
            case ProtocolConstants.TYPE_BLOB:
                mySQLType = fromBlob(columnMeta, properties);
                break;
            case ProtocolConstants.TYPE_BOOL:
                mySQLType = MySQLType.BOOLEAN;
                break;
            case ProtocolConstants.TYPE_GEOMETRY:
                mySQLType = MySQLType.GEOMETRY;
                break;
            default:
                mySQLType = MySQLType.UNKNOWN;
        }
        return mySQLType;
    }

    private static MySQLType fromTiny(MySQLColumnMeta columnMeta, Properties<PropertyKey> properties) {
        // Adjust for pseudo-boolean
        final boolean unsigned = columnMeta.isUnsigned();
        final MySQLType mySQLType;
        if (columnMeta.isTiny1AsBit()
                && properties.getOrDefault(PropertyKey.tinyInt1isBit, Boolean.class)) {
            if (properties.getOrDefault(PropertyKey.transformedBitIsBoolean, Boolean.class)) {
                mySQLType = MySQLType.BOOLEAN;
            } else {
                mySQLType = MySQLType.BIT;
            }
        } else {
            mySQLType = unsigned ? MySQLType.TINYINT_UNSIGNED : MySQLType.TINYINT;
        }
        return mySQLType;
    }


    private static MySQLType fromVarcharOrVarString(MySQLColumnMeta columnMeta, Properties<PropertyKey> properties) {
        final MySQLType mySQLType;
        if (columnMeta.isEnum()) {
            mySQLType = MySQLType.ENUM;
        } else if (columnMeta.isSetType()) {
            mySQLType = MySQLType.SET;
        } else if (isOpaqueBinary(columnMeta)
                && !isFunctionsNeverReturnBlobs(columnMeta, properties)) {
            mySQLType = MySQLType.VARBINARY;
        } else {
            mySQLType = MySQLType.VARCHAR;
        }
        return mySQLType;
    }

    private static MySQLType fromBlob(MySQLColumnMeta columnMeta, Properties<PropertyKey> properties) {
        // Sometimes MySQL uses this protocol-level type for all possible BLOB variants,
        // we can divine what the actual type is by the length reported


        final MySQLType mySQLType;

        final long maxLength = columnMeta.length;
        // fixing initial type according to length
        if (maxLength <= 255L) {
            mySQLType = fromTinyBlob(columnMeta, properties);
        } else if (columnMeta.length <= (1 << 16) - 1) {
            if (columnMeta.isBinary() || !isBlobTypeReturnText(columnMeta, properties)) {
                mySQLType = MySQLType.BLOB;
            } else {
                mySQLType = MySQLType.TEXT;
            }
        } else if (maxLength <= (1 << 24) - 1) {
            mySQLType = fromMediumBlob(columnMeta, properties);
        } else {
            mySQLType = fromLongBlob(columnMeta, properties);
        }
        return mySQLType;
    }

    private static MySQLType fromTinyBlob(MySQLColumnMeta columnMeta, Properties<PropertyKey> properties) {
        final MySQLType mySQLType;
        if (columnMeta.isBinary() || !isBlobTypeReturnText(columnMeta, properties)) {
            mySQLType = MySQLType.TINYBLOB;
        } else {
            mySQLType = MySQLType.TINYTEXT;
        }
        return mySQLType;
    }

    private static MySQLType fromMediumBlob(MySQLColumnMeta columnMeta, Properties<PropertyKey> properties) {
        final MySQLType mySQLType;
        if (columnMeta.isBinary() || !isBlobTypeReturnText(columnMeta, properties)) {
            mySQLType = MySQLType.MEDIUMBLOB;
        } else {
            mySQLType = MySQLType.MEDIUMTEXT;
        }
        return mySQLType;
    }

    private static MySQLType fromLongBlob(MySQLColumnMeta columnMeta, Properties<PropertyKey> properties) {
        final MySQLType mySQLType;
        if (columnMeta.isBinary() || !isBlobTypeReturnText(columnMeta, properties)) {
            mySQLType = MySQLType.LONGBLOB;
        } else {
            mySQLType = MySQLType.LONGTEXT;
        }
        return mySQLType;
    }

    private static MySQLType fromString(MySQLColumnMeta columnMeta, Properties<PropertyKey> properties) {
        final MySQLType mySQLType;
        if (columnMeta.isEnum()) {
            mySQLType = MySQLType.ENUM;
        } else if (columnMeta.isSetType()) {
            mySQLType = MySQLType.SET;
        } else if (columnMeta.isBinary()
                || (isOpaqueBinary(columnMeta)
                && !properties.getOrDefault(PropertyKey.blobsAreStrings, Boolean.class))) {
            mySQLType = MySQLType.BINARY;
        } else {
            mySQLType = MySQLType.CHAR;
        }
        return mySQLType;
    }

    private static boolean isOpaqueBinary(MySQLColumnMeta columnMeta) {

        boolean isImplicitTemporaryTable = columnMeta.tableName != null
                && columnMeta.tableName.startsWith("#sql_"); //TODO check tableName or tableAlias

        boolean isBinaryString = columnMeta.isBinary()
                && columnMeta.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary
                && (columnMeta.typeFlag == ProtocolConstants.TYPE_STRING
                || columnMeta.typeFlag == ProtocolConstants.TYPE_VAR_STRING
                || columnMeta.typeFlag == ProtocolConstants.TYPE_VARCHAR);

        return isBinaryString
                // queries resolved by temp tables also have this 'signature', check for that
                ? !isImplicitTemporaryTable : columnMeta.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary;

    }

    private static boolean isFunctionsNeverReturnBlobs(MySQLColumnMeta columnMeta, Properties<PropertyKey> properties) {
        return StringUtils.isEmpty(columnMeta.tableName)
                && properties.getOrDefault(PropertyKey.functionsNeverReturnBlobs, Boolean.class);
    }

    private static boolean isBlobTypeReturnText(MySQLColumnMeta columnMeta, Properties<PropertyKey> properties) {
        return !columnMeta.isBinary()
                || columnMeta.collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                || properties.getOrDefault(PropertyKey.blobsAreStrings, Boolean.class)
                || isFunctionsNeverReturnBlobs(columnMeta, properties);
    }


}
