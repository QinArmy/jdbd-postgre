package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.vendor.result.ResultRowSink;
import io.jdbd.vendor.result.ResultSetReader;
import io.netty.buffer.ByteBuf;

import java.util.Objects;
import java.util.function.Consumer;

final class DefaultResultSetReader implements ResultSetReader {

    private static final byte[] ZERO_BYTE = new byte[0];


    private final StmtTask stmtTask;

    final TaskAdjutant adjutant;

    private final ResultRowSink sink;

    PgRowMeta rowMeta;

    private Phase phase = Phase.READ_ROW_META;

    private DefaultResultSetReader(StmtTask stmtTask, ResultRowSink sink) {
        this.stmtTask = stmtTask;
        this.adjutant = stmtTask.adjutant();
        this.sink = sink;
    }

    @Override
    public final boolean read(final ByteBuf cumulateBuffer, final Consumer<Object> statesConsumer) {
        boolean resultSetEnd = false, continueRead = true;
        while (continueRead) {
            switch (this.phase) {
                case READ_ROW_META: {
                    this.rowMeta = PgRowMeta.read(cumulateBuffer, this.adjutant);
                    if (this.rowMeta.getColumnCount() == 0) {
                        this.phase = Phase.READ_RESULT_TERMINATOR;
                    } else {
                        this.phase = Phase.READ_ROWS;
                    }
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case READ_ROWS: {
                    if (readRowData(cumulateBuffer)) {
                        this.phase = Phase.READ_RESULT_TERMINATOR;
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }
                }
                break;
                case READ_RESULT_TERMINATOR: {
                    continueRead = false;
                    resultSetEnd = true;
                }
                break;
                case END:
                    throw new IllegalStateException(String.format("%s can't reuse.", this));
                default:
                    throw PgExceptions.createUnknownEnumException(this.phase);
            }
        }

        return resultSetEnd;
    }

    @Override
    public final boolean isResettable() {
        return false;
    }


    /**
     * @return true : read row data end.
     * @see #read(ByteBuf, Consumer)
     */
    private boolean readRowData(final ByteBuf cumulateBuffer) {
        final PgRowMeta rowMeta = Objects.requireNonNull(this.rowMeta, "this.rowMeta");
        final PgColumnMeta[] columnMetaArray = rowMeta.columnMetaArray;
        final ResultRowSink sink = this.sink;
        final boolean isCanceled = sink.isCancelled();

        Object[] columnValueArray;
        int bodyIndex, nextRowIndex;
        while (Messages.hasOneMessage(cumulateBuffer)) {
            if (cumulateBuffer.getByte(cumulateBuffer.readerIndex()) != Messages.D) {
                return true;
            }
            cumulateBuffer.readByte(); // skip message type byte
            bodyIndex = cumulateBuffer.readerIndex();
            nextRowIndex = bodyIndex + cumulateBuffer.readInt();

            if (cumulateBuffer.readShort() != columnMetaArray.length) {
                String m = String.format("Server RowData message column count[%s] and RowDescription[%s] not match."
                        , cumulateBuffer.getShort(cumulateBuffer.readerIndex() - 2), columnMetaArray.length);
                throw new PgJdbdException(m);
            }

            if (isCanceled) {
                cumulateBuffer.readerIndex(nextRowIndex);// skip row
                continue;
            }
            columnValueArray = new Object[columnMetaArray.length];
            PgColumnMeta meta;
            byte[] valueBytes;
            for (int i = 0, valueLength; i < columnMetaArray.length; i++) {
                valueLength = cumulateBuffer.readInt();
                if (valueLength == -1) {
                    // -1 indicates a NULL column value.
                    continue;
                }

                if (valueLength == 0) {
                    valueBytes = ZERO_BYTE;
                } else {
                    valueBytes = new byte[valueLength];
                    cumulateBuffer.readBytes(valueBytes);
                }
                meta = columnMetaArray[i];
                if (meta.textFormat) {
                    columnValueArray[i] = convertTextToColumn(valueBytes, meta);
                } else {
                    columnValueArray[i] = convertBinaryToColumn(valueBytes, meta);
                }

            }

            sink.next(PgResultRow.create(rowMeta, columnValueArray, this.adjutant));

            cumulateBuffer.readerIndex(nextRowIndex);//avoid tail filler

        }

        return false;
    }


    private Object convertTextToColumn(final byte[] valueBytes, final PgColumnMeta meta) {

        return null;
    }

    private Object convertBinaryToColumn(final byte[] valueBytes, final PgColumnMeta meta) {

        return null;
    }


    enum Phase {
        READ_ROW_META,
        READ_ROWS,
        READ_RESULT_TERMINATOR,
        END
    }

}
