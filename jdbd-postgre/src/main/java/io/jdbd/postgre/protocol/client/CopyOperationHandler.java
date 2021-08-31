package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;

/**
 * @see AbstractStmtTask
 */
interface CopyOperationHandler {

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyInResponse</a>
     */
    void handleCopyInResponse(ByteBuf cumulateBuffer);

    /**
     * @return true: copy out handle end.
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyOutResponse</a>
     */
    boolean handleCopyOutResponse(ByteBuf cumulateBuffer);

    /**
     * @return true: copy out end
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyData</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyDone</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">CopyFail</a>
     */
    boolean handleCopyOutData(ByteBuf cumulateBuffer);


}
