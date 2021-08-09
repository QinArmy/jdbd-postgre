package io.jdbd.postgre.syntax;

import io.jdbd.postgre.ServerParameter;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStringUtils;
import org.qinarmy.util.FastStack;
import org.qinarmy.util.Stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

final class DefaultPgParser implements PgParser {

    static DefaultPgParser create(Function<ServerParameter, String> paramFunction) {
        return new DefaultPgParser(paramFunction);
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPgParser.class);

    private static final String BLOCK_COMMENT_START_MARKER = "/*";

    private static final String BLOCK_COMMENT_END_MARKER = "*/";

    private static final String DOUBLE_DASH_COMMENT_MARKER = "--";

    private static final char QUOTE = '\'';

    private static final char DOUBLE_QUOTE = '"';

    private static final char BACK_SLASH = '\\';

    private final Function<ServerParameter, String> paramFunction;


    private DefaultPgParser(Function<ServerParameter, String> paramFunction) {
        this.paramFunction = paramFunction;
    }

    @Override
    public final PgStatement parse(final String sql) throws SQLException {
        return (PgStatement) doParse(sql, true);
    }

    @Override
    public final boolean isSingle(String sql) throws SQLException {
        return (Boolean) doParse(sql, false);
    }

    private Object doParse(final String sql, final boolean createStmt) throws SQLException {
        if (!PgStringUtils.hasText(sql)) {
            throw PgExceptions.createSyntaxError("Statement couldn't be empty.");
        }
        final boolean isTrace = LOG.isTraceEnabled();
        final long startMillis = isTrace ? System.currentTimeMillis() : 0;
        final boolean confirmStringOff = confirmStringIsOff();
        final int sqlLength = sql.length();

        boolean inQuoteString = false, inCStyleEscapes = false, inUnicodeEscapes = false, inDoubleIdentifier = false;
        int stmtCount = 1;
        char ch;
        List<String> endpointList = new ArrayList<>();
        int lastParamEnd = 0;
        for (int i = 0; i < sqlLength; i++) {
            ch = sql.charAt(i);
            if (inQuoteString) {
                int index = sql.indexOf(QUOTE, i);
                if (index < 0) {
                    String m = String.format("syntax error,string constants not close at near %s .", fragment(sql, i));
                    throw PgExceptions.createSyntaxError(m);
                }
                if ((confirmStringOff || inCStyleEscapes) && sql.charAt(index - 1) == BACK_SLASH) {
                    // C-Style Escapes
                    i = index;
                } else if (index + 1 < sqlLength && sql.charAt(index + 1) == QUOTE) {
                    // double quote Escapes
                    i = index + 1;
                } else {
                    i = index;
                    // LOG.debug("QUOTE end c-style[{}] unicode[{}]  ,current char[{}] at near {}",inCStyleEscapes,inUnicodeEscapes,ch,fragment(sql,i));
                    inQuoteString = false; // string constant end.
                    if (inUnicodeEscapes) {
                        inUnicodeEscapes = false;
                    } else if (inCStyleEscapes) {
                        inCStyleEscapes = false;
                    }
                }
                continue;
            } else if (inDoubleIdentifier) {
                int index = sql.indexOf(DOUBLE_QUOTE, i);
                if (index < 0) {
                    String m = String.format(
                            "syntax error,double quoted identifier not close,at near %s", fragment(sql, i));
                    throw PgExceptions.createSyntaxError(m);
                }
                inDoubleIdentifier = false;
                if (inUnicodeEscapes) {
                    inUnicodeEscapes = false;
                }
                // LOG.debug("DOUBLE_QUOTE end ,current char[{}] sql fragment :{}",ch, fragment(sql,index));
                i = index;
                continue;
            }

            if (ch == QUOTE) {
                inQuoteString = true;
                //LOG.debug("QUOTE start ,current char[{}] sql fragment :{}",ch, fragment(sql,i));
            } else if ((ch == 'E' || ch == 'e') && i + 1 < sqlLength && sql.charAt(i + 1) == QUOTE) {
                inQuoteString = inCStyleEscapes = true;
                //LOG.debug("QUOTE c-style start ,current char[{}] sql fragment :{}",ch, fragment(sql,i));
                i++;
            } else if ((ch == 'U' || ch == 'u')
                    && i + 1 < sqlLength && sql.charAt(i + 1) == '&'
                    && i + 2 < sqlLength && sql.charAt(i + 2) == QUOTE) {
                inQuoteString = inUnicodeEscapes = true;
                // LOG.debug("QUOTE unicode start ,current char[{}] sql fragment :{}",ch, fragment(sql,i));
                i += 2;
            } else if (ch == DOUBLE_QUOTE) {
                //LOG.debug("DOUBLE_QUOTE current char[{}] sql fragment :{}",ch, fragment(sql,i));
                inDoubleIdentifier = true;
            } else if ((ch == 'U' || ch == 'u')
                    && i + 1 < sqlLength && sql.charAt(i + 1) == '&'
                    && i + 2 < sqlLength && sql.charAt(i + 2) == DOUBLE_QUOTE) {
                // LOG.debug("DOUBLE_QUOTE with unicode ,current char[{}] sql fragment :{}",ch, fragment(sql,i));
                inDoubleIdentifier = inUnicodeEscapes = true;
                i += 2;
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
            } else if (sql.startsWith(BLOCK_COMMENT_START_MARKER, i)) {
                i = skipBlockComment(sql, i);
            } else if (sql.startsWith(DOUBLE_DASH_COMMENT_MARKER, i)) {
                int index = sql.indexOf('\n', i + DOUBLE_DASH_COMMENT_MARKER.length());
                i = index > 0 ? index : sqlLength;
            } else if (ch == '?') {
                if (createStmt) {
                    endpointList.add(sql.substring(lastParamEnd, i));
                    lastParamEnd = i + 1;
                }
            } else if (ch == ';') {
                if (createStmt) {
                    String m = String.format(
                            "Detect multiple statements,multiple statements can't be bind,please check [%s].", sql);
                    throw PgExceptions.createSyntaxError(m);
                } else {
                    stmtCount++;
                }
            }

        }

        if (inQuoteString) {
            throw PgExceptions.createSyntaxError("syntax error,last string constants not close.");
        }
        if (inDoubleIdentifier) {
            throw PgExceptions.createSyntaxError("syntax error,last double quoted identifier not close.");
        }
        final Object parseResult;
        if (createStmt) {
            if (lastParamEnd < sqlLength) {
                endpointList.add(sql.substring(lastParamEnd));
            } else {
                endpointList.add("");
            }
            parseResult = PgStatementImpl.create(sql, endpointList);
        } else {
            parseResult = stmtCount == 1;
        }

        if (isTrace) {
            LOG.trace("SQL[{}] \nparse cost {} ms.", sql, System.currentTimeMillis() - startMillis);
        }
        return parseResult;
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


    private static String fragment(String sql, int index) {
        return sql.substring(Math.max(index - 10, 0), Math.min(index + 10, sql.length()));
    }


}
