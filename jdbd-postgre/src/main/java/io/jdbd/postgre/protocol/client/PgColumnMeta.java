package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgType;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
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

    final int getTimePrecision() {
        int precision;
        switch (this.columnTypeOid) {
            case PgConstant.TYPE_TIME:
            case PgConstant.TYPE_TIMETZ:
            case PgConstant.TYPE_TIMESTAMP:
            case PgConstant.TYPE_TIMESTAMPTZ: {
                switch (this.columnModifier) {
                    case -1:
                        precision = 6;
                        break;
                    case 1:
                        // Bizarrely SELECT '0:0:0.1'::time(1); returns 2 digits.
                        precision = 2 + 1;
                        break;
                    case 0:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                        precision = this.columnModifier;
                        break;
                    default:
                        throw createServerResponseError();
                }
            }
            break;
            default:
                throw new IllegalStateException(String.format("ColumnMeta[%s] not time or timestamp type.", this));
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
