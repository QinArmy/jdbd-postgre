package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.vendor.conf.Properties;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Objects;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html"> Column Definition Flags</a>
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html"> Column Definition Protocol</a>
 */
public final class MySQLColumnMeta {

    static final MySQLColumnMeta[] EMPTY = new MySQLColumnMeta[0];

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
        // mysqlType must be last
        this.mysqlType = MySQLType.from(this, properties);
    }

    public boolean isUnsigned() {
        return (this.definitionFlags & UNSIGNED_FLAG) != 0;
    }

    long obtainPrecision(Map<Integer, CharsetMapping.CustomCollation> customCollationMap) {
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
            case YEAR:
            case DATE:
                precision = 0L;
                break;
            case TIME:
                precision = obtainTimeTypePrecision(this);
                break;
            case TIMESTAMP:
            case DATETIME:
                precision = obtainDateTimeTypePrecision(this);
                break;
            default:
                precision = -1;

        }
        return precision;
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

    static int obtainTimeTypePrecision(MySQLColumnMeta columnMeta) {
        final int precision;
        if (columnMeta.decimals > 0 && columnMeta.decimals < 7) {
            precision = columnMeta.decimals;
        } else {
            precision = (int) (columnMeta.length - 11L);
            if (precision < 0 || precision > 6) {
                throw new IllegalArgumentException(String.format("MySQLColumnMeta[%s] isn't time type.", columnMeta));
            }
        }
        return precision;
    }

    static int obtainDateTimeTypePrecision(MySQLColumnMeta columnMeta) {
        final int precision;
        if (columnMeta.decimals > 0 && columnMeta.decimals < 7) {
            precision = columnMeta.decimals;
        } else {
            precision = (int) (columnMeta.length - 20L);
            if (precision < 0 || precision > 6) {
                throw new IllegalArgumentException(String.format("MySQLColumnMeta[%s] isn't time type.", columnMeta));
            }
        }
        return precision;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html">Protocol::ColumnDefinition41</a>
     */
    static MySQLColumnMeta readFor41(ByteBuf payloadBuf, ClientProtocolAdjutant adjutant) {
        final Charset metaCharset = adjutant.obtainCharsetMeta();
        final Properties<PropertyKey> properties = adjutant.obtainHostInfo().getProperties();
        // 1. catalog
        String catalogName = PacketUtils.readStringLenEnc(payloadBuf, metaCharset);
        // 2. schema
        String schemaName = PacketUtils.readStringLenEnc(payloadBuf, metaCharset);
        // 3. table,virtual table name
        String tableAlias = PacketUtils.readStringLenEnc(payloadBuf, metaCharset);
        // 4. org_table,physical table name
        String tableName = PacketUtils.readStringLenEnc(payloadBuf, metaCharset);

        // 5. name ,virtual column name,alias in select statement
        String columnAlias = Objects.requireNonNull(PacketUtils.readStringLenEnc(payloadBuf, metaCharset)
                , "columnAlias");
        // 6. org_name,physical column name
        String columnName = PacketUtils.readStringLenEnc(payloadBuf, metaCharset);
        // 7. length of fixed length fields ,[0x0c]
        //
        long fixLength = PacketUtils.readLenEnc(payloadBuf);
        // 8. character_set of column
        int collationIndex = PacketUtils.readInt2(payloadBuf);
        Charset columnCharset = CharsetMapping.getJavaCharsetByCollationIndex(collationIndex
                , adjutant.obtainCustomCollationMap());
        // 9. column_length,maximum length of the field
        long length = PacketUtils.readInt4AsLong(payloadBuf);
        // 10. type,type of the column as defined in enum_field_types,type of the column as defined in enum_field_types
        int typeFlag = PacketUtils.readInt1(payloadBuf);
        // 11. flags,Flags as defined in Column Definition Flags
        int definitionFlags = PacketUtils.readInt2(payloadBuf);
        // 12. decimals,max shown decimal digits:
        //0x00 for integers and static strings
        //0x1f for dynamic strings, double, float
        //0x00 to 0x51 for decimals
        short decimals = (short) PacketUtils.readInt1(payloadBuf);

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


}
