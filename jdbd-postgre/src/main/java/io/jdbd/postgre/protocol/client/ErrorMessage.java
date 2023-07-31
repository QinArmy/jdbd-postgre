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
        int readIndex = message.readerIndex();
        return readBody(message, readIndex + message.readInt(), charset);
    }


    static ErrorMessage readBody(final ByteBuf messageBody, final int nextMsgIndex, Charset charset) {
        return new ErrorMessage(MultiFieldMessage.readMultiFields(messageBody, nextMsgIndex, charset));
    }


    private ErrorMessage(Map<Byte, String> messageParts) {
        super(Messages.E, messageParts);
    }


    public String getMessage() {
        String message;
        message = this.fieldMap.get(MultiFieldMessage.MESSAGE);
        if (message == null) {
            message = "";
        }
        return message;
    }


    @Nullable
    public String getSQLState() {
        return this.fieldMap.get(MultiFieldMessage.SQLSTATE);
    }


    @Nullable
    String getSeverity() {
        return this.fieldMap.get(MultiFieldMessage.SEVERITY);
    }

    @Nullable
    String getDetail() {
        return this.fieldMap.get(MultiFieldMessage.DETAIL);
    }

    @Nullable
    String getHint() {
        return this.fieldMap.get(MultiFieldMessage.HINT);
    }

    @Nullable
    public int getPosition() {
        return getIntegerPart(MultiFieldMessage.POSITION);
    }

    @Nullable
    String getWhere() {
        return this.fieldMap.get(MultiFieldMessage.WHERE);
    }

    @Nullable
    String getSchema() {
        return this.fieldMap.get(MultiFieldMessage.SCHEMA);
    }

    @Nullable
    String getTable() {
        return this.fieldMap.get(MultiFieldMessage.TABLE);
    }

    @Nullable
    String getColumn() {
        return this.fieldMap.get(MultiFieldMessage.COLUMN);
    }

    @Nullable
    String getDatatype() {
        return this.fieldMap.get(MultiFieldMessage.DATATYPE);
    }

    @Nullable
    String getConstraint() {
        return this.fieldMap.get(MultiFieldMessage.CONSTRAINT);
    }

    @Nullable
    String getFile() {
        return this.fieldMap.get(MultiFieldMessage.FILE);
    }

    int getLine() {
        return getIntegerPart(MultiFieldMessage.LINE);
    }

    @Nullable
    String getRoutine() {
        return this.fieldMap.get(MultiFieldMessage.ROUTINE);
    }

    @Nullable
    String getInternalQuery() {
        return this.fieldMap.get(MultiFieldMessage.INTERNAL_QUERY);
    }

    String getNonSensitiveErrorMessage() {
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
        String s = this.fieldMap.get(type);
        int integer;
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
        byte type;

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
