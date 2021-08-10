package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ErrorResponse</a>
 * @see <a href="https://www.postgresql.org/docs/current/protocol-error-fields.html">Error and Notice Message Fields</a>
 */
public final class ErrorMessage extends MultiFieldMessage {


    static ErrorMessage read(final ByteBuf message, Charset charset) {
        if (message.readByte() != Messages.E) {
            char type = (char) message.getByte(message.readerIndex() - 1);
            String msg = String.format("Message type[%s] isn't ErrorResponse.", type);
            throw new IllegalArgumentException(msg);
        }
        final int readIndex = message.readerIndex();
        return readBody(message, readIndex + message.readInt(), charset);
    }


    static ErrorMessage readBody(final ByteBuf messageBody, final int nextMsgIndex, Charset charset) {
        return new ErrorMessage(MultiFieldMessage.readMultiFields(messageBody, nextMsgIndex, charset));
    }


    private ErrorMessage(Map<Byte, String> messageParts) {
        super(Messages.E, messageParts);
    }


    @Nullable
    public final String getSQLState() {
        return this.fieldMap.get(MultiFieldMessage.SQLSTATE);
    }


    @Nullable
    final String getSeverity() {
        return this.fieldMap.get(MultiFieldMessage.SEVERITY);
    }

    @Nullable
    final String getDetail() {
        return this.fieldMap.get(MultiFieldMessage.DETAIL);
    }

    @Nullable
    final String getHint() {
        return this.fieldMap.get(MultiFieldMessage.HINT);
    }

    @Nullable
    public int getPosition() {
        return getIntegerPart(MultiFieldMessage.POSITION);
    }

    @Nullable
    final String getWhere() {
        return this.fieldMap.get(MultiFieldMessage.WHERE);
    }

    @Nullable
    final String getSchema() {
        return this.fieldMap.get(MultiFieldMessage.SCHEMA);
    }

    @Nullable
    final String getTable() {
        return this.fieldMap.get(MultiFieldMessage.TABLE);
    }

    @Nullable
    final String getColumn() {
        return this.fieldMap.get(MultiFieldMessage.COLUMN);
    }

    @Nullable
    final String getDatatype() {
        return this.fieldMap.get(MultiFieldMessage.DATATYPE);
    }

    @Nullable
    final String getConstraint() {
        return this.fieldMap.get(MultiFieldMessage.CONSTRAINT);
    }

    @Nullable
    final String getFile() {
        return this.fieldMap.get(MultiFieldMessage.FILE);
    }

    final int getLine() {
        return getIntegerPart(MultiFieldMessage.LINE);
    }

    @Nullable
    final String getRoutine() {
        return this.fieldMap.get(MultiFieldMessage.ROUTINE);
    }

    @Nullable
    final String getInternalQuery() {
        return this.fieldMap.get(MultiFieldMessage.INTERNAL_QUERY);
    }

    final String getNonSensitiveErrorMessage() {
        StringBuilder builder = new StringBuilder();
        String message = this.fieldMap.get(MultiFieldMessage.SEVERITY);
        if (message != null) {
            builder.append(message).append(": ");
        }
        message = this.fieldMap.get(MultiFieldMessage.MESSAGE);
        if (message != null) {
            builder.append(message);
        }
        return builder.toString();
    }


    private int getIntegerPart(byte type) {
        final String s = this.fieldMap.get(type);
        final int integer;
        if (s == null) {
            integer = 0;
        } else {
            integer = Integer.parseInt(s);
        }
        return integer;
    }


    private enum ErrorField {

        SEVERITY(MultiFieldMessage.SEVERITY),
        SEVERITY_V(MultiFieldMessage.SEVERITY_V),
        SQLSTATE(MultiFieldMessage.SQLSTATE),
        MESSAGE(MultiFieldMessage.MESSAGE),

        DETAIL(MultiFieldMessage.DETAIL),
        HINT(MultiFieldMessage.HINT),
        POSITION(MultiFieldMessage.POSITION),
        INTERNAL_POSITION(MultiFieldMessage.INTERNAL_POSITION),

        INTERNAL_QUERY(MultiFieldMessage.INTERNAL_QUERY),
        WHERE(MultiFieldMessage.WHERE),
        SCHEMA(MultiFieldMessage.SCHEMA),
        TABLE(MultiFieldMessage.TABLE),

        COLUMN(MultiFieldMessage.COLUMN),
        DATATYPE(MultiFieldMessage.DATATYPE),
        CONSTRAINT(MultiFieldMessage.CONSTRAINT),
        FILE(MultiFieldMessage.FILE),

        LINE(MultiFieldMessage.LINE),
        ROUTINE(MultiFieldMessage.ROUTINE);

        final byte type;

        ErrorField(byte type) {
            this.type = type;
        }

        @Nullable
        static ErrorField fromType(final byte type) {
            for (ErrorField value : ErrorField.values()) {
                if (type == value.type) {
                    return value;
                }
            }
            return null;
        }


    }


}
