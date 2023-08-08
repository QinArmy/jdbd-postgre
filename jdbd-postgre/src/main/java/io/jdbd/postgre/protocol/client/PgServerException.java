package io.jdbd.postgre.protocol.client;

import io.jdbd.result.ServerException;
import io.jdbd.session.Option;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * <p>
 * Emit, when postgre server return error message.
 * </p>
 *
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ErrorResponse</a>
 * @see <a href="https://www.postgresql.org/docs/current/protocol-error-fields.html">Error and Notice Message Fields</a>
 * @since 1.0
 */
final class PgServerException extends ServerException {

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ErrorResponse</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-error-fields.html">Error and Notice Message Fields</a>
     */
    static PgServerException read(final ByteBuf cumulateBuffer, final Charset charset) {
        if (cumulateBuffer.readByte() != Messages.E) {
            char type = (char) cumulateBuffer.getByte(cumulateBuffer.readerIndex() - 1);
            String msg = String.format("Message type[%s] isn't ErrorResponse.", type);
            throw new IllegalArgumentException(msg);
        }
        return readBody(cumulateBuffer, cumulateBuffer.readerIndex() + cumulateBuffer.readInt(), charset);
    }

    static PgServerException readBody(final ByteBuf cumulateBuffer, final int endIndex, Charset charset) {
        final Map<Byte, Object> fieldMap;
        fieldMap = MultiFieldMessage.readFields(cumulateBuffer, endIndex, charset);
        return new PgServerException(fieldMap);
    }


    private final Map<Byte, Object> fieldMap;

    /**
     * private constructor
     */
    private PgServerException(Map<Byte, Object> fieldMap) {
        super((String) fieldMap.getOrDefault(MultiFieldMessage.MESSAGE, ""), (String) fieldMap.get(MultiFieldMessage.SQLSTATE), 0);
        this.fieldMap = fieldMap;
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T> T valueOf(final Option<T> option) {
        final Byte code;
        code = MultiFieldMessage.OPTION_MAP.get(option);
        if (code == null) {
            return null;
        }
        return (T) this.fieldMap.get(code);
    }

    @Override
    public String toString() {
        final String className = PgServerException.class.getName();
        final StringBuilder builder = new StringBuilder(className.length() + this.fieldMap.size() * 10);
        builder.append(className)
                .append("[");
        MultiFieldMessage.appendFieldToString(builder, this.fieldMap, true);
        return builder.append(",\nhash=")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }



}
