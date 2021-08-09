package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.netty.buffer.ByteBuf;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">RowDescription</a>
 */
final class PgColumnMeta {

    static final PgColumnMeta[] EMPTY = new PgColumnMeta[0];


    final String columnAlias;

    final int tableOid;

    final short columnAttrNum;

    final int columnTypeOid;

    final short columnTypeSize;

    final int columnModifier;

    final boolean textFormat;

    final PgType pgType;


    private PgColumnMeta(String columnAlias, int tableOid
            , short columnAttrNum, int columnTypeOid
            , short columnTypeSize, int columnModifier
            , boolean textFormat, PgType pgType) {

        this.columnAlias = columnAlias;
        this.tableOid = tableOid;
        this.columnAttrNum = columnAttrNum;
        this.columnTypeOid = columnTypeOid;

        this.columnTypeSize = columnTypeSize;
        this.columnModifier = columnModifier;
        this.textFormat = textFormat;
        this.pgType = pgType;
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


        final PgColumnMeta[] columnMetas = new PgColumnMeta[columnCount];

        for (int i = 0; i < columnCount; i++) {

            String columnAlias = Messages.readString(message);
            int tableOid = message.readInt();
            short columnAttrNum = message.readShort();
            int columnTypeOid = message.readInt();

            short columnTypeSize = message.readShort();
            int columnModifier = message.readInt();
            boolean textFormat = message.readShort() == 0;
            PgType pgType = parseToPgType(columnTypeOid, columnTypeSize, columnModifier, adjutant);

            columnMetas[i] = new PgColumnMeta(columnAlias, tableOid
                    , columnAttrNum, columnTypeOid
                    , columnTypeSize, columnModifier
                    , textFormat, pgType);
        }

        message.readerIndex(nextMsgIndex);//avoid tail filler
        return columnMetas;
    }

    private static PgType parseToPgType(int columnTypeOid, short columnTypeSize
            , int columnModifier, TaskAdjutant adjutant) {
        throw new UnsupportedOperationException();
    }


}
