package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ReadyForQuery</a>
 */
enum TxStatus {
    /**
     * idle,not in a transaction block
     */
    IDLE,
    /**
     * in a transaction block
     */
    TRANSACTION,
    /**
     * in a failed transaction block (queries will be rejected until block is ended).
     */
    ERROR;

    static TxStatus from(byte statusByte) {
        final TxStatus status;
        switch (statusByte) {
            case Messages.I:
                status = TxStatus.IDLE;
                break;
            case Messages.T:
                status = TxStatus.TRANSACTION;
                break;
            case Messages.E:
                status = TxStatus.ERROR;
                break;
            default:
                throw new IllegalArgumentException(String.format("Unknown transaction status[%s].", (char) statusByte));
        }
        return status;
    }

    static TxStatus read(final ByteBuf message) {
        if (message.readByte() != Messages.Z) {
            throw new IllegalArgumentException("Non ReadyForQuery message.");
        }
        final int readIndex, nextMsgIndex;
        readIndex = message.readerIndex();
        nextMsgIndex = readIndex + message.readInt();

        final TxStatus status;
        status = from(message.readByte());

        message.readerIndex(nextMsgIndex); // avoid tail filler
        return status;
    }


}
