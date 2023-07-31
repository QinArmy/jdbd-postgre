package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">NoticeResponse</a>
 * @see <a href="https://www.postgresql.org/docs/current/protocol-error-fields.html">Error and Notice Message Fields</a>
 */
final class NoticeMessage extends MultiFieldMessage {

    static NoticeMessage read(ByteBuf message, Charset charset) {
        if (message.readByte() != Messages.N) {
            throw new IllegalArgumentException("Non Notice message.");
        }
        int index = message.readerIndex();
        return new NoticeMessage(readMultiFields(message, index + message.readInt(), charset));
    }

    static NoticeMessage readBody(ByteBuf message, final int nextMsgIndex, Charset charset) {
        return new NoticeMessage(readMultiFields(message, nextMsgIndex, charset));
    }

    private NoticeMessage(Map<Byte, String> fieldMap) {
        super(Messages.N, fieldMap);
    }


    @Nullable
    public String getMessage() {
        return this.fieldMap.get(MultiFieldMessage.MESSAGE);
    }

}
