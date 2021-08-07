package io.jdbd.postgre.syntax;

import io.jdbd.postgre.ServerParameter;
import io.jdbd.postgre.util.PgExceptions;
import org.qinarmy.util.FastStack;
import org.qinarmy.util.Stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Function;

final class DefaultPgParser implements PgParser {


    private static final Logger LOG = LoggerFactory.getLogger(DefaultPgParser.class);

    private static final String BLOCK_COMMENT_START_MARKER = "/*";

    private static final String BLOCK_COMMENT_END_MARKER = "*/";

    private static final String DOUBLE_DASH_COMMENT_MARKER = "--";

    private static final char QUOTE = '\'';

    private static final char DOUBLE_QUOTE = '"';

    private static final char BACK_SLASH = '\\';

    private final Function<ServerParameter, String> paramFunction = s -> "";


    private DefaultPgParser() {

    }

    @Override
    public final PgStatement parse(final String sql) throws SQLException {
        boolean inQuoteString = false, inCStyleEscapes = false, inUnicodeEscapes = false, inDoubleIdentifier = false;

        final boolean confirmStringOff = confirmStringIsOff();
        final int sqlLength = sql.length();
        char ch;
        for (int i = 0; i < sqlLength; i++) {
            ch = sql.charAt(i);
            if (inQuoteString) {
                int index = sql.indexOf(QUOTE, i + 1);
                if (index < 0) {
                    throw PgExceptions.createSyntaxError("syntax error,string constants not close.");
                }
                if ((confirmStringOff || inCStyleEscapes) && sql.charAt(index - 1) == BACK_SLASH) {
                    i = index;
                } else if (i + 1 < sqlLength && sql.charAt(i + 1) == QUOTE) {

                }
                if (ch == QUOTE) {

                    if (i + 1 < sqlLength && sql.charAt(i + 1) == QUOTE) {
                        i++;
                        continue;
                    }
                    inQuoteString = false; // string constant end.
                    if (inUnicodeEscapes) {
                        inUnicodeEscapes = false;
                    } else if (inCStyleEscapes) {
                        inCStyleEscapes = false;
                    }
                } else if (ch == BACK_SLASH && (confirmStringOff || inCStyleEscapes)) {
                    // backslash is valid
                    i++;
                }
                continue;
            } else if (inDoubleIdentifier) {
                if (ch == DOUBLE_QUOTE) {
                    inDoubleIdentifier = false;
                    if (inUnicodeEscapes) {
                        inUnicodeEscapes = false;
                    }
                }
                continue;
            }

            if (Character.isWhitespace(ch)) {
                continue;
            }

            if (ch == QUOTE) {
                inQuoteString = true;
            } else if ((ch == 'E' || ch == 'e') && i + 1 < sqlLength && sql.charAt(i + 1) == QUOTE) {
                inQuoteString = inCStyleEscapes = true;
            } else if ((ch == 'U' || ch == 'u')
                    && i + 1 < sqlLength && sql.charAt(i + 1) == '&'
                    && i + 2 < sqlLength && sql.charAt(i + 2) == QUOTE) {
                inQuoteString = inUnicodeEscapes = true;
            } else if (ch == DOUBLE_QUOTE) {
                inDoubleIdentifier = true;
            } else if ((ch == 'U' || ch == 'u')
                    && i + 1 < sqlLength && sql.charAt(i + 1) == '&'
                    && i + 2 < sqlLength && sql.charAt(i + 2) == DOUBLE_QUOTE) {
                inDoubleIdentifier = inUnicodeEscapes = true;
            } else if (ch == '$') {
                // Dollar-Quoted String Constants
                int index = sql.indexOf('$', i + 1);
                if (index < 0) {
                    throw PgExceptions.createSyntaxError("syntax error at or near \"$\"");
                }
                final String dollarTag = sql.substring(i, index + 1);
                index = sql.indexOf(dollarTag, index + 1);
                if (index < 0) {
                    String msg = String.format(
                            "syntax error,Dollar-Quoted String Constants not close, at or near \"%s\"", dollarTag);
                    throw PgExceptions.createSyntaxError(msg);
                }
                i = index + dollarTag.length() - 1;
            } else if (sql.startsWith(DOUBLE_DASH_COMMENT_MARKER, i)) {
                i = skipBlockComment(sql, i);
            } else if (sql.startsWith(BLOCK_COMMENT_START_MARKER, i)) {
                int index = sql.indexOf('\n', i + DOUBLE_DASH_COMMENT_MARKER.length());
                i = index > 0 ? index : sqlLength;
            }
        }
        return null;
    }


    private boolean confirmStringIsOff() {
        String status = this.paramFunction.apply(ServerParameter.standard_conforming_strings);
        Objects.requireNonNull(status, "standard_conforming_strings value");
        return !ServerParameter.isOn(status);
    }

    /**
     * @return {@link #BLOCK_COMMENT_END_MARKER}'s last char index.
     */
    private static int skipBlockComment(final String sql, final int firstStartMarkerIndex)
            throws SQLException {

        final int length = sql.length(), markerLength = BLOCK_COMMENT_START_MARKER.length();
        final Stack<String> stack = new FastStack<>();
        final String errorMsg = "Block comment marker quote(/*) not close.";

        stack.push(BLOCK_COMMENT_START_MARKER);
        for (int i = firstStartMarkerIndex + markerLength, startMarkerIndex, endMarkerIndex; i < length; ) {
            endMarkerIndex = sql.indexOf(BLOCK_COMMENT_END_MARKER, i);
            if (endMarkerIndex < 0) {
                throw PgExceptions.createSyntaxError(errorMsg);
            }
            startMarkerIndex = sql.indexOf(BLOCK_COMMENT_START_MARKER, i);
            if (startMarkerIndex > 0 && startMarkerIndex < endMarkerIndex) {
                // nest, push start marker
                stack.push(BLOCK_COMMENT_START_MARKER);
            }
            stack.pop();
            if (stack.isEmpty()) {
                return endMarkerIndex + markerLength - 1;
            }
            // nest ,continue search
            i = endMarkerIndex + markerLength;
        }
        throw PgExceptions.createSyntaxError(errorMsg);
    }

    /**
     * @return the index of line end char.
     */
    private static int skipLineComment(final String sql, final int searchIndex) {
        final int length = sql.length();
        int index = sql.indexOf('\n', searchIndex);
        if (index < 0) {
            index = sql.indexOf('\r', searchIndex);
        }
        int i;
        if (index < 0) {
            i = sql.length();
        } else {
            i = index;
        }
        return i;
    }


}
