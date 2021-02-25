package io.jdbd.mysql.syntax;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.vendor.util.JdbdStringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DefaultMySQLParser implements MySQLParser {

    public static MySQLParser create(Server server) {
        return new DefaultMySQLParser(server);
    }


    private static final String BLOCK_COMMENT_START_MARKER = "/*";

    private static final String BLOCK_COMMENT_END_MARKER = "*/";

    private static final String POUND_COMMENT_MARKER = "#";

    private static final String DOUBLE_DASH_COMMENT_MARKER = "-- ";

    private static final char QUOTE = '\'';

    private static final char DOUBLE_QUOTE = '"';

    private static final char IDENTIFIER_QUOTE = '`';

    private static final char BACK_SLASH = '\\';

    private static final String LOAD = "LOAD";

    private static final String DATA = "DATA";

    private static final String XML = "XML";

    private static final String LOCAL = "LOCAL";


    private final Server server;

    private DefaultMySQLParser(Server server) {
        this.server = server;
    }

    @Override
    public final MySQLStatement parse(final String sql) throws JdbdSQLException {
        if (!JdbdStringUtils.hasText(sql)) {
            throw MySQLExceptionUtils.createEmptySqlException();
        }
        Object value = doParse(sql, Mode.PARSE);
        if (value instanceof MySQLStatement) {
            return (MySQLStatement) value;
        }
        throw new IllegalStateException("parser bug.");
    }

    @Override
    public boolean isSingleStmt(String sql) {
        Object value;
        try {
            value = doParse(sql, Mode.SINGLE);
        } catch (JdbdSQLException e) {
            return false;
        }
        if (value instanceof Integer) {
            return ((Integer) value) == 1;
        }
        throw new IllegalStateException("parser bug.");
    }

    @Override
    public boolean isMultiStmt(String sql) {
        Object value;
        try {
            value = doParse(sql, Mode.MULTI);
        } catch (JdbdSQLException e) {
            return false;
        }
        if (value instanceof Integer) {
            return ((Integer) value) > 1;
        }
        throw new IllegalStateException("parser bug.");
    }

    private Object doParse(final String sql, final Mode mode) throws JdbdSQLException {

        final boolean ansiQuotes = this.server.containSqlMode(SQLMode.ANSI_QUOTES);
        final boolean backslashEscapes = this.server.isBackslashEscapes();

        boolean inQuotes = false, inDoubleQuotes = false, inQuoteId = false, inDoubleId = false, localInfile = false;
        final int sqlLength = sql.length();
        int lastParmEnd = 0, stmtCount = 0;
        final List<String> endpointList = mode == Mode.PARSE ? new ArrayList<>() : Collections.emptyList();

        char ch, firstStmtChar = Constants.EMPTY_CHAR, letter = Constants.EMPTY_CHAR;
        for (int i = 0; i < sqlLength; i++) {
            ch = sql.charAt(i);

            if (inQuoteId) {
                if (ch == IDENTIFIER_QUOTE) {
                    inQuoteId = false;
                } else if (ch == BACK_SLASH && backslashEscapes) {
                    i++;// next character is escaped
                }
                continue;
            } else if (inDoubleId) {
                // ANSI_QUOTES mode enable,double quote(")  are interpreted as identifiers.
                if (ch == DOUBLE_QUOTE) {
                    inDoubleId = false;
                } else if (ch == BACK_SLASH && backslashEscapes) {
                    i++;// next character is escaped
                }
                continue;
            } else if (inQuotes) {
                //  only respect quotes when not in a quoted identifier
                if (ch == QUOTE) {
                    inQuotes = false;
                } else if (ch == BACK_SLASH && backslashEscapes) {
                    i++;// next character is escaped
                }
                continue;
            } else if (inDoubleQuotes) {
                if (ch == DOUBLE_QUOTE) {
                    inDoubleQuotes = false;
                } else if (ch == BACK_SLASH && backslashEscapes) {
                    i++;// next character is escaped
                }
                continue;
            }

            if (Character.isWhitespace(ch)) {
                continue;
            }

            if (sql.startsWith(BLOCK_COMMENT_START_MARKER, i)) {
                i = skipBlockCommentEndMarker(sql, i);
                continue;
            } else if (sql.startsWith(POUND_COMMENT_MARKER, i)) {
                i = skipCommentLineEnd(sql, POUND_COMMENT_MARKER, i);
                continue;
            } else if (sql.startsWith(DOUBLE_DASH_COMMENT_MARKER, i)) {
                i = skipCommentLineEnd(sql, DOUBLE_DASH_COMMENT_MARKER, i);
                continue;
            }

            if (ch == IDENTIFIER_QUOTE) {
                inQuoteId = true;
            } else if (ch == DOUBLE_QUOTE) {
                if (ansiQuotes) {
                    inDoubleId = true;
                } else {
                    inDoubleQuotes = true;
                }
            } else if (ch == QUOTE) {
                inQuotes = true;
            } else if (ch == ';') {
                switch (mode) {
                    case PARSE:
                        throw MySQLExceptionUtils.createMultiStatementException();
                    case SINGLE:
                        return 2;// not single statement.
                    case MULTI: {
                        if (letter != Constants.EMPTY_CHAR) {
                            letter = Constants.EMPTY_CHAR;
                            stmtCount++;
                        }
                    }
                    break;
                    default:
                        throw MySQLExceptionUtils.createUnknownEnumException(mode);
                }
            } else if (ch == '?') {
                if (mode == Mode.PARSE) {
                    endpointList.add(sql.substring(lastParmEnd, i));
                    lastParmEnd = i + 1;
                }
            } else if (letter == Constants.EMPTY_CHAR && Character.isLetter(ch)) {
                letter = Character.toUpperCase(ch);
                if (firstStmtChar == Constants.EMPTY_CHAR) {
                    firstStmtChar = letter;
                    if ((ch == 'L') && !localInfile) {
                        if (sql.regionMatches(true, i, LOAD, 0, LOAD.length())) {
                            localInfile = isLocalInfile(sql, i);
                        }
                    }
                }

            }


        }


        if (inQuoteId) {
            throw MySQLExceptionUtils.createSyntaxException("Identifier quote(`) not close.");
        } else if (inDoubleId) {
            throw MySQLExceptionUtils.createSyntaxException("Identifier quote(\") not close.");
        } else if (inQuotes) {
            throw MySQLExceptionUtils.createSyntaxException("String Literals quote(') not close.");
        } else if (inDoubleQuotes) {
            throw MySQLExceptionUtils.createSyntaxException("String Literals double quote(\") not close.");
        }

        final Object returnValue;
        switch (mode) {
            case SINGLE:
            case MULTI: {
                if (stmtCount == 0) {
                    stmtCount = 1;
                }
                returnValue = stmtCount;
            }
            break;
            case PARSE: {
                if (lastParmEnd < sqlLength) {
                    endpointList.add(sql.substring(lastParmEnd, sqlLength));
                } else {
                    endpointList.add("");
                }
                returnValue = new DefaultMySQLStatement(sql, endpointList, localInfile);
            }
            break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(mode);
        }

        return returnValue;

    }


    /*################################## blow private static method ##################################*/


    private static boolean isLocalInfile(final String sql, final int i) {
        final int sqlLength = sql.length();

        int wordIndex = i + LOAD.length();
        if (wordIndex >= sqlLength || !Character.isWhitespace(sql.charAt(wordIndex))) {
            return false;
        }

        for (int j = wordIndex; j < sqlLength; j++) {
            if (!Character.isWhitespace(sql.charAt(j))) {
                wordIndex = j;
                break;
            }
        }

        boolean match = false;
        if (sql.regionMatches(true, wordIndex, DATA, 0, DATA.length())) {
            match = true;
            wordIndex += DATA.length();
        } else if (sql.regionMatches(true, wordIndex, XML, 0, XML.length())) {
            match = true;
            wordIndex += XML.length();
        }
        if (!match || wordIndex >= sqlLength || !Character.isWhitespace(sql.charAt(wordIndex))) {
            return false;
        }

        for (int j = wordIndex; j < sqlLength; j++) {
            if (!Character.isWhitespace(sql.charAt(j))) {
                wordIndex = j;
                break;
            }
        }
        match = false;
        if (sql.regionMatches(true, wordIndex, LOCAL, 0, LOCAL.length())) {
            wordIndex += LOCAL.length();
            if (wordIndex < sqlLength && Character.isWhitespace(sql.charAt(wordIndex))) {
                match = true;
            }
        }
        return match;
    }


    /**
     * @return {@link #BLOCK_COMMENT_END_MARKER}'s last char index.
     */
    private static int skipBlockCommentEndMarker(String sql, int i)
            throws JdbdSQLException {
        i += BLOCK_COMMENT_START_MARKER.length();
        if (i >= sql.length()) {
            throw MySQLExceptionUtils.createSyntaxException("Block comment marker quote(/*) not close.");
        }
        int endMarkerIndex = sql.indexOf(BLOCK_COMMENT_END_MARKER, i);
        if (endMarkerIndex < 0) {
            throw MySQLExceptionUtils.createSyntaxException("Block comment marker quote(/*) not close.");
        }
        return endMarkerIndex + BLOCK_COMMENT_END_MARKER.length() - 1;
    }


    /**
     * @return the index of line end char.
     */
    private static int skipCommentLineEnd(final String sql, final String lineCommentMarker, int i)
            throws JdbdSQLException {
        i += lineCommentMarker.length();
        if (i >= sql.length()) {
            i = sql.length();
        } else {
            int index = sql.indexOf('\n', i);
            if (index < 0) {
                index = sql.indexOf('\r', i);
            }
            if (index < 0) {
                i = sql.length();
            } else {
                i = index;
            }
        }
        return i;
    }


    private enum Mode {
        PARSE,
        SINGLE,
        MULTI
    }


}
