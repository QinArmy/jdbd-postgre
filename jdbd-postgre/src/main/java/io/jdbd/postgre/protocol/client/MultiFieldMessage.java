package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.session.Option;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

/**
 * @see NoticeMessage
 */
abstract class MultiFieldMessage extends PgMessage {


    /**
     * below postgre Error and Notice Message Fields
     *
     * @see <a href="https://www.postgresql.org/docs/current/protocol-error-fields.html">Error and Notice Message Fields</a>
     */
    static final byte SEVERITY_S = 'S';
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

    static final Map<Option<String>, Byte> OPTION_MAP = createOptionMap();

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


    static void appendFieldToString(final StringBuilder builder, final Map<Byte, ?> fieldMap,
                                    final boolean firstItem) {
        final String line = System.lineSeparator();
        int index = 0;
        for (Map.Entry<Byte, ?> e : fieldMap.entrySet()) {
            if (index > 0 || !firstItem) {
                builder.append(PgConstant.COMMA);
            }
            builder.append(line)
                    .append(keyByteAsFieldName(e.getKey()))
                    .append("=")
                    .append(e.getValue());

            index++;

        }
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
     * @return a unmodified map, key : field name,value : field value.
     */
    static Map<Byte, String> readFields(final ByteBuf messageBody, final int endIndex, Charset charset) {
        final Map<Byte, String> map = PgCollections.hashMap();

        byte field;
        while (messageBody.readerIndex() < endIndex) {
            field = messageBody.readByte();
            if (field == 0) {
                break;
            }
            map.put(field, Messages.readString(messageBody, charset));
        }
        messageBody.readerIndex(endIndex);// avoid filler.
        return Collections.unmodifiableMap(map);
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
        Map<Byte, String> map = PgCollections.hashMap((int) (18 / 0.75));

        map.put(SEVERITY_S, "SEVERITY");
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


    /**
     * @return a unmodified map.
     */
    private static Map<Option<String>, Byte> createOptionMap() {
        final Map<Option<String>, Byte> map = PgCollections.hashMap((int) (18 / 0.75));

        map.put(PgErrorOrNotice.PG_MSG_SEVERITY_S, SEVERITY_S);
        map.put(PgErrorOrNotice.PG_MSG_SEVERITY_V, SEVERITY_V);
        map.put(Option.SQL_STATE, SQLSTATE);
        map.put(Option.MESSAGE, MESSAGE);

        map.put(PgErrorOrNotice.PG_MSG_DETAIL, DETAIL);
        map.put(PgErrorOrNotice.PG_MSG_HINT, HINT);
        map.put(PgErrorOrNotice.PG_MSG_POSITION, POSITION);
        map.put(PgErrorOrNotice.PG_MSG_INTERNAL_POSITION, INTERNAL_POSITION);

        map.put(PgErrorOrNotice.PG_MSG_INTERNAL_QUERY, INTERNAL_QUERY);
        map.put(PgErrorOrNotice.PG_MSG_WHERE, WHERE);
        map.put(PgErrorOrNotice.PG_MSG_SCHEMA, SCHEMA);
        map.put(PgErrorOrNotice.PG_MSG_TABLE, TABLE);

        map.put(PgErrorOrNotice.PG_MSG_COLUMN, COLUMN);
        map.put(PgErrorOrNotice.PG_MSG_DATATYPE, DATATYPE);
        map.put(PgErrorOrNotice.PG_MSG_CONSTRAINT, CONSTRAINT);
        map.put(PgErrorOrNotice.PG_MSG_FILE, FILE);

        map.put(PgErrorOrNotice.PG_MSG_LINE, LINE);
        map.put(PgErrorOrNotice.PG_MSG_ROUTINE, ROUTINE);

        return Collections.unmodifiableMap(map);
    }


}
