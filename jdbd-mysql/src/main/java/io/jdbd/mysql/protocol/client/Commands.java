package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.util.MySQLStrings;

import java.util.List;

abstract class Commands {

    Commands() {
        throw new UnsupportedOperationException();
    }

    static final String CHARACTER_SET_CLIENT = "character_set_client";
    static final String CHARACTER_SET_RESULTS = "character_set_results";
    static final String COLLATION_CONNECTION = "collation_connection";
    static final String RESULTSET_METADATA = "resultset_metadata";


    static String buildSetVariableCommand(String pairString) {
        List<String> pairList = MySQLStrings.split(pairString, ",;", "\"'(", "\"')");
        StringBuilder builder = new StringBuilder("SET ");
        int index = 0;
        for (String pair : pairList) {
            pair = pair.trim();
            if (!pair.contains("=")) {
                throw new MySQLJdbdException("Property sessionVariables format error,please check it.");
            }
            String lower = pair.toLowerCase();
            if (lower.contains(CHARACTER_SET_RESULTS)
                    || lower.contains(CHARACTER_SET_CLIENT)
                    || lower.contains(COLLATION_CONNECTION)
                    || lower.contains(RESULTSET_METADATA)) {

                throw createSetVariableException();
            }
            if (index > 0) {
                builder.append(",");
            }
            if (!pair.contains("@")) {
                builder.append("@@SESSION.");
            }
            builder.append(pair);
            index++;
        }
        return builder.toString();
    }

    /*################################## blow private method ##################################*/

    /**
     * @see #buildSetVariableCommand(String)
     */
    private static MySQLJdbdException createSetVariableException() {
        String message = String.format("Below three session variables[%s,%s,%s,%s] must specified by below three properties[%s,%s,%s]."
                , CHARACTER_SET_CLIENT
                , CHARACTER_SET_RESULTS
                , COLLATION_CONNECTION
                , RESULTSET_METADATA
                , MyKey.characterEncoding
                , MyKey.characterSetResults
                , MyKey.connectionCollation);
        return new MySQLJdbdException(message);
    }


}
