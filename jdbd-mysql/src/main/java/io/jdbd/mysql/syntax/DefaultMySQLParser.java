package io.jdbd.mysql.syntax;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.util.JdbdStrings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public final class DefaultMySQLParser implements MySQLParser {


    public static MySQLParser create(Function<SQLMode, Boolean> sqlModeFunction) {
        return new DefaultMySQLParser(sqlModeFunction);
    }


    public static MySQLParser getForInitialization() {
        return INITIALIZING_INSTANCE;
    }

    /*
     * not java-doc
     * @see ClientConnectionProtocolImpl#authenticateAndInitializing()
     */
    private static boolean initSqlModeFunction(SQLMode sqlMode) {
        boolean contains;
        switch (sqlMode) {
            case ANSI_QUOTES:
            case NO_BACKSLASH_ESCAPES:
                contains = false;
                break;
            default:
                throw new IllegalArgumentException("sqlMode error");
        }
        return contains;
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMySQLParser.class);

    private static final DefaultMySQLParser INITIALIZING_INSTANCE = new DefaultMySQLParser(DefaultMySQLParser::initSqlModeFunction);

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


    private final Function<SQLMode, Boolean> sqlModeFunction;

    private DefaultMySQLParser(Function<SQLMode, Boolean> sqlModeFunction) {
        this.sqlModeFunction = sqlModeFunction;
    }

    @Override
    public final MySQLStatement parse(final String sql) throws SQLException {
        Object value = doParse(sql, Mode.PARSE);
        if (value instanceof MySQLStatement) {
            return (MySQLStatement) value;
        }
        throw new IllegalStateException("parser bug.");
    }

    @Override
    public boolean isSingleStmt(String sql) throws SQLException {
        Object value;
        value = doParse(sql, Mode.SINGLE);
        if (value instanceof Integer) {
            return ((Integer) value) == 1;
        }
        throw new IllegalStateException("parser bug.");
    }

    @Override
    public boolean isMultiStmt(String sql) throws SQLException {
        Object value;
        value = doParse(sql, Mode.MULTI);
        if (value instanceof Integer) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("statement count value is {}", value);
            }
            return ((Integer) value) > 1;
        }
        throw new IllegalStateException("parser bug.");
    }

    private Object doParse(final String sql, final Mode mode) throws SQLException {
        if (!JdbdStrings.hasText(sql)) {
            throw MySQLExceptions.createEmptySqlException();
        }
        final boolean ansiQuotes = this.sqlModeFunction.apply(SQLMode.ANSI_QUOTES);
        final boolean backslashEscapes = !this.sqlModeFunction.apply(SQLMode.NO_BACKSLASH_ESCAPES);

        boolean inQuotes = false, inDoubleQuotes = false, inQuoteId = false, inDoubleId = false, localInfile = false;
        final int sqlLength = sql.length();
        int lastParmEnd = 0, stmtCount = 0;
        final List<String> endpointList = mode == Mode.PARSE ? new ArrayList<>() : Collections.emptyList();

        char ch, firstStmtChar = Constants.EMPTY_CHAR, firstEachStmt = Constants.EMPTY_CHAR;
        for (int i = 0, line = 1; i < sqlLength; i++) {
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
                    //TODO 需要检测 前面是不 BACK_SLASH
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

            if (ch == '\r') {
                if (i + 1 < sqlLength && sql.charAt(i + 1) == '\n') {
                    i++;
                }
                line++;
                continue;
            } else if (ch == '\n') {
                line++;
                continue;
            } else if (Character.isWhitespace(ch)) {
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
                        throw MySQLExceptions.createMultiStatementError();
                    case SINGLE:
                        return 2;// not single statement.
                    case MULTI: {
                        stmtCount++;
                        if (firstEachStmt != Constants.EMPTY_CHAR) {
                            firstEachStmt = Constants.EMPTY_CHAR;
                        }
                    }
                    break;
                    default:
                        throw MySQLExceptions.createUnexpectedEnumException(mode);
                }
            } else if (ch == '?') {
                if (mode == Mode.PARSE) {
                    endpointList.add(sql.substring(lastParmEnd, i));
                    lastParmEnd = i + 1;
                }
            } else if (firstEachStmt == Constants.EMPTY_CHAR && Character.isLetter(ch)) {
                firstEachStmt = Character.toUpperCase(ch);
                if (firstStmtChar == Constants.EMPTY_CHAR) {
                    firstStmtChar = firstEachStmt;
                    //TODO  search for "ON DUPLICATE KEY UPDATE" if not an INSERT statement
                    //@see com.mysql.cj.ParseInfo.ParseInfo(java.lang.String, com.mysql.cj.Session, java.lang.String, boolean)
                    if (ch == 'L' && sql.regionMatches(true, i, LOAD, 0, LOAD.length())) {
                        localInfile = isLocalInfile(sql, i);

                    }
                }

            }


        }


        if (inQuoteId) {
            throw MySQLExceptions.createSyntaxError("Identifier quote(`) not close.");
        } else if (inDoubleId) {
            throw MySQLExceptions.createSyntaxError("Identifier quote(\") not close.");
        } else if (inQuotes) {
            throw MySQLExceptions.createSyntaxError("String Literals quote(') not close.");
        } else if (inDoubleQuotes) {
            throw MySQLExceptions.createSyntaxError("String Literals double quote(\") not close.");
        }

        final Object returnValue;
        switch (mode) {
            case SINGLE:
            case MULTI: {
                if (stmtCount == 0) {
                    stmtCount = 1;
                } else if (mode == Mode.MULTI && stmtCount > 0 && firstEachStmt != Constants.EMPTY_CHAR) {
                    stmtCount++;
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
                throw MySQLExceptions.createUnexpectedEnumException(mode);
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
            throws SQLException {
        i += BLOCK_COMMENT_START_MARKER.length();
        if (i >= sql.length()) {
            throw MySQLExceptions.createSyntaxError("Block comment marker quote(/*) not close.");
        }
        int endMarkerIndex = sql.indexOf(BLOCK_COMMENT_END_MARKER, i);
        if (endMarkerIndex < 0) {
            throw MySQLExceptions.createSyntaxError("Block comment marker quote(/*) not close.");
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


    /*################################## blow private static method ##################################*/


    private enum Mode {
        PARSE,
        SINGLE,
        MULTI
    }


}
