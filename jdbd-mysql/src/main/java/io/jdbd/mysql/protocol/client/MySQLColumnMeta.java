package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.util.MySQLObjects;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html"> Column Definition Flags</a>
 */
final class MySQLColumnMeta {

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
            , int collationIndex, long fixedLength
            , long length, int typeFlag
            , int definitionFlags, short decimals, Properties properties) {

        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableAlias = tableAlias;

        this.columnName = columnName;
        this.columnAlias = columnAlias;
        this.collationIndex = collationIndex;
        this.fixedLength = fixedLength;

        this.length = length;
        this.typeFlag = typeFlag;
        this.definitionFlags = definitionFlags;
        this.decimals = decimals;
        // mysqlType must be last
        this.mysqlType = MySQLType.from(this, properties);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html">Protocol::ColumnDefinition41</a>
     */
    static MySQLColumnMeta readFor41(ByteBuf payloadBuf, Charset metaCharset, Properties properties) {
        // 1. catalog
        String catalogName = PacketUtils.readStringLenEnc(payloadBuf, metaCharset);
        // 2. schema
        String schemaName = PacketUtils.readStringLenEnc(payloadBuf, metaCharset);
        // 3. table,virtual table name
        String tableAlias = PacketUtils.readStringLenEnc(payloadBuf, metaCharset);
        // 4. org_table,physical table name
        String tableName = PacketUtils.readStringLenEnc(payloadBuf, metaCharset);

        // 5. name ,virtual column name,alias in select statement
        String columnAlias = MySQLObjects.requireNonNull(PacketUtils.readStringLenEnc(payloadBuf, metaCharset)
                , "columnAlias");
        // 6. org_name,physical column name
        String columnName = PacketUtils.readStringLenEnc(payloadBuf, metaCharset);
        // 7. length of fixed length fields
        long fixedLength = PacketUtils.readLenEnc(payloadBuf);
        // 8. character_set of column
        int collationIndex = PacketUtils.readInt2(payloadBuf);

        // 9. column_length,maximum length of the field
        long length = PacketUtils.readInt4(payloadBuf);
        // 10. type,type of the column as defined in enum_field_types,type of the column as defined in enum_field_types
        int typeFlag = PacketUtils.readInt1(payloadBuf);
        // 11. flags,Flags as defined in Column Definition Flags
        int definitionFlags = PacketUtils.readInt2(payloadBuf);
        // 12. decimals,max shown decimal digits:
        //0x00 for integers and static strings
        //0x1f for dynamic strings, double, float
        //0x00 to 0x51 for decimals
        short decimals = PacketUtils.readInt1(payloadBuf);

        return new MySQLColumnMeta(
                catalogName, schemaName
                , tableName, tableAlias
                , columnName, columnAlias
                , collationIndex, fixedLength
                , length, typeFlag
                , definitionFlags, decimals
                , properties
        );
    }


}
