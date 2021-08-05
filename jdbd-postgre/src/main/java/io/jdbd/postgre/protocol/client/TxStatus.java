package io.jdbd.postgre.protocol.client;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ReadyForQuery</a>
 */
enum TxStatus {
    /** idle,not in a transaction block */
    IDLE,
    /** in a transaction block */
    TRANSACTION,
    /** in a failed transaction block (queries will be rejected until block is ended). */
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


}
