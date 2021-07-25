package io.jdbd.postgre.protocol.client;

import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @see <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">ErrorResponse</a>
 * @see <a href="https://www.postgresql.org/docs/11/protocol-error-fields.html">Error and Notice Message Fields</a>
 */
public final class ErrorMessage extends PostgreMessage {


    static ErrorMessage read(final ByteBuf message) {
        if (message.readByte() != Messages.E) {
            throw new IllegalArgumentException("payload isn't error message.");
        }
        final int startIndex = message.readerIndex();
        final int length = message.readInt(), end = message.readerIndex() + length - 4;
        final Map<Byte, String> map = new HashMap<>();

        byte field;
        while (message.readerIndex() < end) {
            field = message.readByte();
            if (field == 0) {
                break;
            }
            map.put(field, Messages.readString(message));
        }
        message.readerIndex(startIndex + length);// avoid filler.
        return new ErrorMessage(Messages.E, map);
    }


    static final byte SEVERITY = 'S';
    static final byte SEVERITY_V = 'V';
    static final byte SQLSTATE = 'C';
    static final byte MESSAGE = 'M';

    static final byte DETAIL = 'D';
    static final byte HINT = 'H';
    static final byte POSITION = 'P';
    static final byte INTERNAL_POSITION = 'p';

    static final byte INTERNAL_QUERY = 'q';
    static final byte WHERE = 'W';
    static final byte SCHEMA = 's';
    static final byte TABLE = 't';

    static final byte COLUMN = 'c';
    static final byte DATATYPE = 'd';
    static final byte CONSTRAINT = 'n';
    static final byte FILE = 'F';

    static final byte LINE = 'L';
    static final byte ROUTINE = 'R';

    final Map<Byte, String> messageParts;

    private ErrorMessage(byte type, Map<Byte, String> messageParts) {
        super(type);
        this.messageParts = Collections.unmodifiableMap(messageParts);
    }


    @Override
    public final String toString() {
        StringBuilder builder = new StringBuilder("ErrorPacket{");
        final String separator = System.lineSeparator();
        for (Map.Entry<Byte, String> e : this.messageParts.entrySet()) {
            builder.append(separator)
                    .append(keyAsText(e.getKey()))
                    .append("=")
                    .append(e.getValue());

        }
        return builder.toString();
    }


    @Nullable
    public final String getSQLState() {
        return this.messageParts.get(SQLSTATE);
    }

    @Nullable
    public final String getMessage() {
        return this.messageParts.get(MESSAGE);
    }

    @Nullable
    final String getSeverity() {
        return this.messageParts.get(SEVERITY);
    }

    @Nullable
    final String getDetail() {
        return this.messageParts.get(DETAIL);
    }

    @Nullable
    final String getHint() {
        return this.messageParts.get(HINT);
    }

    @Nullable
    public int getPosition() {
        return getIntegerPart(POSITION);
    }

    @Nullable
    final String getWhere() {
        return this.messageParts.get(WHERE);
    }

    @Nullable
    final String getSchema() {
        return this.messageParts.get(SCHEMA);
    }

    @Nullable
    final String getTable() {
        return this.messageParts.get(TABLE);
    }

    @Nullable
    final String getColumn() {
        return this.messageParts.get(COLUMN);
    }

    @Nullable
    final String getDatatype() {
        return this.messageParts.get(DATATYPE);
    }

    @Nullable
    final String getConstraint() {
        return this.messageParts.get(CONSTRAINT);
    }

    @Nullable
    final String getFile() {
        return this.messageParts.get(FILE);
    }

    final int getLine() {
        return getIntegerPart(LINE);
    }

    @Nullable
    final String getRoutine() {
        return this.messageParts.get(ROUTINE);
    }

    @Nullable
    final String getInternalQuery() {
        return this.messageParts.get(INTERNAL_QUERY);
    }

    final String getNonSensitiveErrorMessage() {
        StringBuilder builder = new StringBuilder();
        String message = this.messageParts.get(SEVERITY);
        if (message != null) {
            builder.append(message).append(": ");
        }
        message = this.messageParts.get(MESSAGE);
        if (message != null) {
            builder.append(message);
        }
        return builder.toString();
    }


    private int getIntegerPart(byte type) {
        final String s = this.messageParts.get(type);
        final int integer;
        if (s == null) {
            integer = 0;
        } else {
            integer = Integer.parseInt(s);
        }
        return integer;
    }


    private static String keyAsText(final byte type) {
        ErrorField field = ErrorField.fromType(type);
        return field == null ? Character.toString((char) type) : field.name();
    }


    private enum ErrorField {

        SEVERITY(ErrorMessage.SEVERITY),
        SEVERITY_V(ErrorMessage.SEVERITY_V),
        SQLSTATE(ErrorMessage.SQLSTATE),
        MESSAGE(ErrorMessage.MESSAGE),

        DETAIL(ErrorMessage.DETAIL),
        HINT(ErrorMessage.HINT),
        POSITION(ErrorMessage.POSITION),
        INTERNAL_POSITION(ErrorMessage.INTERNAL_POSITION),

        INTERNAL_QUERY(ErrorMessage.INTERNAL_QUERY),
        WHERE(ErrorMessage.WHERE),
        SCHEMA(ErrorMessage.SCHEMA),
        TABLE(ErrorMessage.TABLE),

        COLUMN(ErrorMessage.COLUMN),
        DATATYPE(ErrorMessage.DATATYPE),
        CONSTRAINT(ErrorMessage.CONSTRAINT),
        FILE(ErrorMessage.FILE),

        LINE(ErrorMessage.LINE),
        ROUTINE(ErrorMessage.ROUTINE);

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
