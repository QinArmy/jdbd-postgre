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
        final Map<Byte, String> fieldMap;
        fieldMap = MultiFieldMessage.readFields(cumulateBuffer, endIndex, charset);
        return new PgServerException(fieldMap);
    }


    private final Map<Byte, String> fieldMap;

    /**
     * private constructor
     */
    private PgServerException(Map<Byte, String> fieldMap) {
        super(fieldMap.getOrDefault(MultiFieldMessage.MESSAGE, ""), fieldMap.get(MultiFieldMessage.SQLSTATE), 0);
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

        final T value;
        if (option.javaType() == Integer.class) {
            value = (T) getIntegerPart(code);
        } else {
            value = (T) this.fieldMap.get(code);
        }
        return value;
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

    private Integer getIntegerPart(final byte type) {
        String s = this.fieldMap.get(type);
        int integer;
        if (s == null) {
            integer = 0;
        } else {
            integer = Integer.parseInt(s);
        }
        return integer;
    }


}
