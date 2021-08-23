package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgType;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
 * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html">Date/Time Types</a>
 */
final class PgColumnMeta {

    static final PgColumnMeta[] EMPTY = new PgColumnMeta[0];


    final String columnLabel;

    final int tableOid;

    final short columnAttrNum;

    final int columnTypeOid;

    final short columnTypeSize;

    final int columnModifier;

    final boolean textFormat;

    final PgType pgType;


    private PgColumnMeta(String columnLabel, int tableOid
            , short columnAttrNum, int columnTypeOid
            , short columnTypeSize, int columnModifier
            , boolean textFormat, PgType pgType) {

        this.columnLabel = columnLabel;
        this.tableOid = tableOid;
        this.columnAttrNum = columnAttrNum;
        this.columnTypeOid = columnTypeOid;

        this.columnTypeSize = columnTypeSize;
        this.columnModifier = columnModifier;
        this.textFormat = textFormat;
        this.pgType = pgType;
    }

    @Override
    public final String toString() {
        return new StringBuilder(PgColumnMeta.class.getSimpleName())
                .append("{")
                .append("\ncolumnLabel=")
                .append(this.columnLabel)
                .append("\npgType=")
                .append(this.pgType)
                .append("\ncolumnTypeId=")
                .append(this.columnTypeOid)
                .append("\ntableOid=")
                .append(this.tableOid)
                .append("\ncolumnNum=")
                .append(this.columnAttrNum)
                .append("\ncolumnTypeSize=")
                .append(this.columnTypeSize)
                .append("\ncolumnModifier=")
                .append(this.columnModifier)
                .append("\ntextFormat=")
                .append(this.textFormat)
                .append("\n}")
                .toString();
    }


    final int getScale() {
        final int scale;
        switch (this.pgType) {
            case DECIMAL:
            case DECIMAL_ARRAY: {
                scale = this.columnModifier == -1 ? 0 : ((this.columnModifier - 4) & 0xFFFF);
            }
            break;
            case TIME:
            case TIMETZ:
            case TIMESTAMP:
            case TIMESTAMPTZ:
            case TIME_ARRAY:
            case TIMETZ_ARRAY:
            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ_ARRAY: {
                scale = this.columnModifier == -1 ? 6 : this.columnModifier;
            }
            break;
            case INTERVAL:
            case INTERVAL_ARRAY: {
                scale = this.columnModifier == -1 ? 6 : (this.columnModifier & 0xFFFF);
            }
            break;
            default:
                scale = 0;

        }
        return scale;
    }

    final int getPrecision() {
        final int precision;
        switch (this.pgType) {
            case SMALLINT:
            case SMALLINT_ARRAY:
                precision = 2;
                break;
            case INTEGER:
            case INTEGER_ARRAY:
            case REAL:
            case REAL_ARRAY:
            case DATE:
            case DATE_ARRAY:
                precision = 4;
                break;
            case BIGINT:
            case BIGINT_ARRAY:
            case DOUBLE:
            case DOUBLE_ARRAY:
            case TIME:
            case TIME_ARRAY:
            case TIMESTAMP:
            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ:
            case TIMESTAMPTZ_ARRAY:
                precision = 8;
                break;
            case DECIMAL:
            case DECIMAL_ARRAY:
                precision = this.columnModifier == -1 ? 0 : (((this.columnModifier - 4) & 0xFFFF0000) >> 16);
                break;
            case CHAR:
            case CHAR_ARRAY: {
                switch (this.columnTypeOid) {
                    case PgConstant.TYPE_CHAR:
                    case PgConstant.TYPE_CHAR_ARRAY:
                        precision = this.columnModifier == -1 ? 1 : (this.columnModifier - 4);
                        break;
                    default:
                        precision = this.columnModifier - 4;
                }
            }
            break;
            case VARCHAR:
            case VARCHAR_ARRAY:
                precision = this.columnModifier - 4;
                break;
            case TIMETZ:
            case TIMETZ_ARRAY:
                precision = 12;
                break;
            case INTERVAL:
            case INTERVAL_ARRAY:
            case UUID:
            case UUID_ARRAY:
            case POINT:
            case POINT_ARRAY:
                precision = 16;
                break;
            case CIRCLE:
            case CIRCLES_ARRAY:
                precision = 24;
                break;
            case BOX:
            case BOX_ARRAY:
            case LINE_SEGMENT:
            case LINE_SEGMENT_ARRAY:
                precision = 32;
                break;
            case BOOLEAN:
            case BOOLEAN_ARRAY:
                precision = 1;
                break;
            case BIT:
            case BIT_ARRAY:
            case VARBIT:
            case VARBIT_ARRAY:
                precision = this.columnModifier;
                break;
            default:
                precision = 0;
        }
        return precision;
    }

    private PgJdbdException createServerResponseError() {
        return new PgJdbdException(String.format("Server response column meat data[%s] error.", this));
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
     */
    static PgColumnMeta[] read(final ByteBuf message, final TaskAdjutant adjutant) {
        if (message.readByte() != Messages.T) {
            throw new IllegalArgumentException("Not RowDescription message.");
        }
        final int bodyIndex = message.readerIndex(), length = message.readInt();
        final int columnCount = message.readShort(), nextMsgIndex = bodyIndex + length;

        if (columnCount == 0) {
            message.readerIndex(nextMsgIndex);//avoid tail filler
            return EMPTY;
        }

        final Charset charset = adjutant.clientCharset();
        final PgColumnMeta[] columnMetas = new PgColumnMeta[columnCount];

        for (int i = 0; i < columnCount; i++) {

            String columnAlias = Messages.readString(message, charset);
            int tableOid = message.readInt();
            short columnAttrNum = message.readShort();
            int columnTypeOid = message.readInt();

            short columnTypeSize = message.readShort();
            int columnModifier = message.readInt();
            boolean textFormat = message.readShort() == 0;

            columnMetas[i] = new PgColumnMeta(columnAlias, tableOid
                    , columnAttrNum, columnTypeOid
                    , columnTypeSize, columnModifier
                    , textFormat, PgType.from(columnTypeOid));
        }

        message.readerIndex(nextMsgIndex);//avoid tail filler
        return columnMetas;
    }


}
