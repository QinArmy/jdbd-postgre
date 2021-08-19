package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">BackendKeyData</a>
 */
final class BackendKeyData extends PgMessage {

    /**
     * Only read The process ID of this backend and The secret key of this backend (not byte1 and length) .
     */
    static BackendKeyData readBody(ByteBuf msgBody) {
        return new BackendKeyData(msgBody.readInt(), msgBody.readInt());
    }


    final int processId;

    final int secretKey;

    private BackendKeyData(int processId, int secretKey) {
        super(Messages.K);
        this.processId = processId;
        this.secretKey = secretKey;
    }


}
