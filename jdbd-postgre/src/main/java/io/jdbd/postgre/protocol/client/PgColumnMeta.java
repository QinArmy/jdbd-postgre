package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.vendor.result.ColumnMeta;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
 * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html">Date/Time Types</a>
 */
final class PgColumnMeta implements ColumnMeta {

    static final PgColumnMeta[] EMPTY = new PgColumnMeta[0];

    final int columnIndex;

    final String columnLabel;

    final int tableOid;

    final short columnAttrNum;

    final int columnTypeOid;

    final short columnTypeSize;

    final int columnModifier;

    final boolean textFormat;

    final DataType dataType;


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
     * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html">Date/Time Types</a>
     */
    private PgColumnMeta(int columnIndex, final ByteBuf cumulateBuffer, final Charset charset,
                         final TaskAdjutant adjutant) {

        this.columnIndex = columnIndex;

        this.columnLabel = Messages.readString(cumulateBuffer, charset);
        this.tableOid = cumulateBuffer.readInt();
        this.columnAttrNum = cumulateBuffer.readShort();
        this.columnTypeOid = cumulateBuffer.readInt();

        this.columnTypeSize = cumulateBuffer.readShort();
        this.columnModifier = cumulateBuffer.readInt();
        this.textFormat = cumulateBuffer.readShort() == 0;

        final PgType pgType;
        pgType = PgType.from(this.columnTypeOid);
        if (pgType == PgType.UNSPECIFIED) {
            this.dataType = adjutant.handleUnknownType(this.columnTypeOid);
        } else {
            this.dataType = pgType;
        }
    }


    @Override
    public String toString() {
        return PgStrings.builder()
                .append(PgColumnMeta.class.getSimpleName())
                .append("{")
                .append("\ncolumnLabel=")
                .append(this.columnLabel)
                .append("\npgType=")
                .append(this.dataType)
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


    @Override
    public int getColumnIndex() {
        return this.columnIndex;
    }

    @Override
    public DataType getDataType() {
        return this.dataType;
    }

    @Override
    public String getColumnLabel() {
        return this.columnLabel;
    }

    @Override
    public boolean isUnsigned() {
        // postgre don't support unsigned
        return false;
    }

    @Override
    public boolean isBit() {
        return this.dataType == PgType.BIT || this.dataType == PgType.VARBIT;
    }

    int getScale() {
        final DataType dataType = this.dataType;
        if (!(dataType instanceof PgType)) {
            return -1;
        }
        final int scale, modifier = this.columnModifier;
        switch ((PgType) dataType) {
            case DECIMAL:
            case DECIMAL_ARRAY:
                scale = modifier == -1 ? 0 : ((modifier - 4) & 0xFFFF);
                break;
            case TIME:
            case TIMETZ:
            case TIMESTAMP:
            case TIMESTAMPTZ:
            case TIME_ARRAY:
            case TIMETZ_ARRAY:
            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ_ARRAY: {
                scale = modifier == -1 ? 6 : modifier;
            }
            break;
            case INTERVAL:
            case INTERVAL_ARRAY: {
                scale = modifier == -1 ? 6 : (modifier & 0xFFFF);
            }
            break;
            default:
                scale = -1;

        }
        return scale;
    }

    int getPrecision() {
        final DataType dataType = this.dataType;
        if (!(dataType instanceof PgType)) {
            return -1;
        }
        final int precision, modifier = this.columnModifier;
        switch ((PgType) dataType) {
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
            case FLOAT8:
            case FLOAT8_ARRAY:
            case TIME:
            case TIME_ARRAY:
            case TIMESTAMP:
            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ:
            case TIMESTAMPTZ_ARRAY:
                precision = 8;
                break;
            case TIMETZ:
            case TIMETZ_ARRAY:
                precision = 12;
                break;
            case DECIMAL:
            case DECIMAL_ARRAY:
                precision = modifier == -1 ? 0 : (((modifier - 4) & 0xFFFF0000) >> 16);
                break;
            case CHAR:
            case CHAR_ARRAY: {
                switch (this.columnTypeOid) {
                    case PgConstant.TYPE_CHAR:
                    case PgConstant.TYPE_CHAR_ARRAY:
                        precision = modifier == -1 ? 1 : (modifier - 4);
                        break;
                    default:
                        precision = modifier - 4;
                }
            }
            break;
            case VARCHAR:
            case VARCHAR_ARRAY:
                precision = modifier - 4;
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
            case CIRCLE_ARRAY:
                precision = 24;
                break;
            case BOX:
            case BOX_ARRAY:
            case LSEG:
            case LSEG_ARRAY:
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
                precision = modifier;
                break;
            default:
                precision = -1;
        }
        return precision;
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
     */
    static PgColumnMeta[] read(final ByteBuf cumulateBuffer, final TaskAdjutant adjutant) {
        if (cumulateBuffer.readByte() != Messages.T) {
            throw new IllegalArgumentException("Not RowDescription message.");
        }
        final int bodyIndex = cumulateBuffer.readerIndex(), length = cumulateBuffer.readInt();
        final int columnCount = cumulateBuffer.readShort(), nextMsgIndex = bodyIndex + length;

        if (columnCount == 0) {
            cumulateBuffer.readerIndex(nextMsgIndex);//avoid tail filler
            return EMPTY;
        }

        final Charset charset = adjutant.clientCharset();
        final PgColumnMeta[] columnMetas = new PgColumnMeta[columnCount];

        for (int i = 0; i < columnCount; i++) {

            columnMetas[i] = new PgColumnMeta(i, cumulateBuffer, charset, adjutant);
        }

        cumulateBuffer.readerIndex(nextMsgIndex);//avoid tail filler
        return columnMetas;
    }


}
