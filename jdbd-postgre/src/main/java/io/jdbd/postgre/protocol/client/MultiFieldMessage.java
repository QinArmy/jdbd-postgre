package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.util.PgCollections;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @see ErrorMessage
 * @see NoticeMessage
 */
abstract class MultiFieldMessage extends PgMessage {


    /**
     * below postgre Error and Notice Message Fields
     *
     * @see <a href="https://www.postgresql.org/docs/current/protocol-error-fields.html">Error and Notice Message Fields</a>
     */
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

    static final Map<Byte, String> FIELD_NAME_MAP = createFieldNameMap();

    final Map<Byte, String> fieldMap;

    MultiFieldMessage(byte type, Map<Byte, String> fieldMap) {
        super(type);
        this.fieldMap = PgCollections.unmodifiableMap(fieldMap);
    }


    @Override
    public final String toString() {
        final String line = System.lineSeparator();
        StringBuilder builder = new StringBuilder(getClass().getSimpleName())
                .append("{");

        for (Map.Entry<Byte, String> e : this.fieldMap.entrySet()) {
            builder.append(line)
                    .append(keyByteAsFieldName(e.getKey()))
                    .append("=")
                    .append(e.getValue());

        }
        return builder.append(line)
                .append("}")
                .toString();
    }


    /*################################## blow packet static method ##################################*/

    /**
     * <p>
     * Read multi filed message body :
     *     <ul>
     *         <li>ErrorResponse</li>
     *         <li>NoticeResponse</li>
     *     </ul>
     * </p>
     *
     * @param endIndex message end index,exclusive.
     * @return map, key : field name,value : field value.
     * @see ErrorMessage
     */
    static Map<Byte, String> readMultiFields(final ByteBuf messageBody, final int endIndex, Charset charset) {
        if (messageBody.readerIndex() >= endIndex) {
            throw new IllegalArgumentException(String.format("readerIndex() >= endIndex[%s]", endIndex));
        }
        Map<Byte, String> map = new HashMap<>();

        byte field;
        while (messageBody.readerIndex() < endIndex) {
            field = messageBody.readByte();
            if (field == 0) {
                break;
            }
            map.put(field, Messages.readString(messageBody, charset));
        }
        messageBody.readerIndex(endIndex);// avoid filler.
        return map;
    }

    /*################################## blow private static method ##################################*/


    private static String keyByteAsFieldName(byte keyByte) {
        String fieldName;
        fieldName = FIELD_NAME_MAP.get(keyByte);
        if (fieldName == null) {
            fieldName = String.valueOf((char) keyByte);
        }
        return fieldName;
    }

    /**
     * @return a unmodified map.
     */
    private static Map<Byte, String> createFieldNameMap() {
        Map<Byte, String> map = new HashMap<>((int) (18 / 0.75));

        map.put(SEVERITY, "SEVERITY");
        map.put(SEVERITY_V, "SEVERITY_V");
        map.put(SQLSTATE, "SQLSTATE");
        map.put(MESSAGE, "MESSAGE");

        map.put(DETAIL, "DETAIL");
        map.put(HINT, "HINT");
        map.put(POSITION, "POSITION");
        map.put(INTERNAL_POSITION, "INTERNAL_POSITION");

        map.put(INTERNAL_QUERY, "INTERNAL_QUERY");
        map.put(WHERE, "WHERE");
        map.put(SCHEMA, "SCHEMA");
        map.put(TABLE, "TABLE");

        map.put(COLUMN, "COLUMN");
        map.put(DATATYPE, "DATATYPE");
        map.put(CONSTRAINT, "CONSTRAINT");
        map.put(FILE, "FILE");

        map.put(LINE, "LINE");
        map.put(ROUTINE, "ROUTINE");

        return Collections.unmodifiableMap(map);
    }


}
