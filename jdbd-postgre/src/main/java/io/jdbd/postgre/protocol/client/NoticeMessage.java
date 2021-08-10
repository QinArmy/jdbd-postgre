package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">NoticeResponse</a>
 * @see <a href="https://www.postgresql.org/docs/current/protocol-error-fields.html">Error and Notice Message Fields</a>
 */
final class NoticeMessage extends MultiFieldMessage {

    /**
     * @param endIndex message end index,exclusive.
     */
    static NoticeMessage readBody(final ByteBuf messageBody, Charset charset) {
        int index = messageBody.readerIndex();
        return new NoticeMessage(MultiFieldMessage.readMultiFields(messageBody, index + messageBody.readInt(), charset));
    }

    private NoticeMessage(Map<Byte, String> fieldMap) {
        super(Messages.N, fieldMap);
    }


}
