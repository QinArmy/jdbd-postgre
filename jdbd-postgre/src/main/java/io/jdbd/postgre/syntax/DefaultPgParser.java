package io.jdbd.postgre.syntax;

import io.jdbd.postgre.ServerParameter;
import io.jdbd.postgre.util.PgExceptions;
import org.qinarmy.util.FastStack;
import org.qinarmy.util.Pair;
import org.qinarmy.util.Stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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

    private static final String COPY = "COPY";

    private static final String FROM = "FROM";

    private static final String TO = "TO";

    private static final String PROGRAM = "PROGRAM";

    private static final String STDIN = "STDIN";

    private static final String STDOUT = "STDOUT";

    private static final char QUOTE = '\'';

    private static final char DOUBLE_QUOTE = '"';

    private static final char BACK_SLASH = '\\';

    private static final char SLASH = '/';

    private static final char STAR = '*';

    private static final char DASH = '-';

    private static final char DOLLAR = '$';

    private final Function<ServerParameter, String> paramFunction;


    private DefaultPgParser(Function<ServerParameter, String> paramFunction) {
        this.paramFunction = paramFunction;
    }

    @Override
    public final PgStatement parse(final String sql) throws SQLException {
        return (PgStatement) doParse(sql, Mode.BIND);
    }

    @SuppressWarnings("unchecked")
    @Override
    public final List<String> separateMultiStmt(final String multiStmt) throws SQLException {
        return (List<String>) doParse(multiStmt, Mode.SEPARATE);
    }

    @Override
    public final CopyIn parseCopyIn(final String sql) throws SQLException {

        final char[] charArray = sql.toCharArray();
        final int lastIndex = charArray.length - 1;
        char ch;
        boolean copyCommand = false, fromClause = false, program = false;
        CopyIn copyIn = null;
        for (int i = 0, bindIndex = 0; i < charArray.length; i++) {
            ch = charArray[i];
            if (Character.isWhitespace(ch)) {
                continue;
            }
            if (ch == SLASH && i < lastIndex && charArray[i + 1] == STAR) {
                // block comment.
                i = skipBlockComment(sql, i);
            } else if (ch == DASH && i < lastIndex && charArray[i + 1] == DASH) {
                // line comment
                int index = sql.indexOf('\n', i + DOUBLE_DASH_COMMENT_MARKER.length());
                i = index > 0 ? index : charArray.length;
            } else if (!copyCommand) {
                if (i == lastIndex
                        || !sql.regionMatches(true, i, COPY, 0, COPY.length())
                        || !Character.isWhitespace(sql.charAt(i + COPY.length()))) {
                    throw PgExceptions.createSyntaxError("Not found COPY command");
                }
                i += COPY.length();
                copyCommand = true;
            } else if (ch == DOUBLE_QUOTE) {
                if (i < lastIndex) {
                    i = sql.indexOf(DOUBLE_QUOTE, i + 1);
                }
                if (i < 0) {
                    throw createDoubleQuotedIdentifierError(sql, i);
                }
            } else if (!fromClause) {
                if ((ch == 'f' || ch == 'F')
                        && Character.isWhitespace(charArray[i - 1])
                        && (i + FROM.length()) < lastIndex
                        && Character.isWhitespace(charArray[i + FROM.length()])
                        && sql.regionMatches(true, i, FROM, 0, FROM.length())) {
                    i += FROM.length();
                    fromClause = true;
                }
            } else if (ch == '?') {
                copyIn = program ? new CopyInFromProgramCommandWithBind(bindIndex)
                        : new CopyInFromLocalFileWithBind(bindIndex);
                break;
            } else if (i + 3 >= lastIndex) {
                String m = String.format(
                        "syntax error,FROM clause error,at near %s", fragment(sql, i));
                throw PgExceptions.createSyntaxError(m);
            } else if (!program
                    && (ch == 'p' || ch == 'P')
                    && sql.regionMatches(true, i, PROGRAM, 0, PROGRAM.length())
                    && Character.isWhitespace(charArray[i + PROGRAM.length()])) {
                // PROGRAM 'command' not supported by client,because command only find in postgre server.
                program = true;
                i += PROGRAM.length();
            } else if ((ch == 's' || ch == 'S')
                    && sql.regionMatches(true, i, STDIN, 0, STDIN.length())
                    && (i + STDIN.length() == lastIndex || Character.isWhitespace(charArray[i + STDIN.length()]))) {
                copyIn = CopyInFromStdin.INSTANCE;
                break;
            } else { // 'filename'
                try {
                    final String constant;
                    constant = parseStringConstant(sql, charArray, i);
                    copyIn = program ? new CopyInFromProgramCommand(constant)
                            : new CopyInFromLocalFile(Paths.get(constant));
                    break;
                } catch (SQLException e) {
                    LOG.debug("COPY IN FROM clause parse filename error.", e);
                    String m = String.format("syntax error,FROM clause error,at near %s", fragment(sql, i));
                    throw PgExceptions.createSyntaxError(m);
                }
            }

        } // for
        if (copyIn == null) {
            throw PgExceptions.createSyntaxError("Not Found FROM clause in COPY.");
        }
        return copyIn;
    }

    @Override
    public final CopyOut parseCopyOut(final String sql) throws SQLException {

        final char[] charArray = sql.toCharArray();
        final int lastIndex = charArray.length - 1;
        char ch;
        boolean copyCommand = false, toClause = false, sourceParsed = false, program = false;
        CopyOut copyOut = null;
        for (int i = 0, bindIndex = 0; i < charArray.length; i++) {
            ch = charArray[i];
            if (Character.isWhitespace(ch)) {
                continue;
            }
            if (ch == SLASH && i < lastIndex && charArray[i + 1] == STAR) {
                // block comment.
                i = skipBlockComment(sql, i);
            } else if (ch == DASH && i < lastIndex && charArray[i + 1] == DASH) {
                // line comment
                int index = sql.indexOf('\n', i + DOUBLE_DASH_COMMENT_MARKER.length());
                i = index > 0 ? index : charArray.length;
            } else if (!copyCommand) {
                if (i == lastIndex
                        || !sql.regionMatches(true, i, COPY, 0, COPY.length())
                        || !Character.isWhitespace(sql.charAt(i + COPY.length()))) {
                    throw PgExceptions.createSyntaxError("Not found COPY command");
                }
                i += COPY.length();
                copyCommand = true;
            } else if (!sourceParsed) {
                if (ch == '(') {
                    // query ,skip query
                    final Pair<Integer, Integer> pair;
                    pair = skipCopyOutQuery(sql, charArray, i, bindIndex);
                    i = pair.getFirst();
                    bindIndex = pair.getSecond();
                }
                sourceParsed = true;
            } else if (ch == DOUBLE_QUOTE) {
                if (i < lastIndex) {
                    i = sql.indexOf(DOUBLE_QUOTE, i + 1);
                }
                if (i < 0) {
                    throw createDoubleQuotedIdentifierError(sql, i);
                }
            } else if (!toClause) {
                if ((ch == 't' || ch == 'T')
                        && Character.isWhitespace(charArray[i - 1])
                        && (i + TO.length()) < lastIndex
                        && Character.isWhitespace(charArray[i + TO.length()])
                        && sql.regionMatches(true, i, TO, 0, TO.length())) {
                    i += TO.length();
                    toClause = true;
                }
            } else if (ch == '?') {
                copyOut = program ? new CopyOutToProgramCommandWithBind(bindIndex)
                        : new CopyOutToLocalFileWithBind(bindIndex);
                break;
            } else if (i + 3 >= lastIndex) {
                String m = String.format(
                        "syntax error,TO clause error,at near %s", fragment(sql, i));
                throw PgExceptions.createSyntaxError(m);
            } else if (!program
                    && (ch == 'p' || ch == 'P')
                    && sql.regionMatches(true, i, PROGRAM, 0, PROGRAM.length())
                    && Character.isWhitespace(charArray[i + PROGRAM.length()])) {
                program = true;
                i += PROGRAM.length();
            } else if ((ch == 's' || ch == 'S')
                    && sql.regionMatches(true, i, STDOUT, 0, STDOUT.length())
                    && (i + STDOUT.length() == lastIndex || Character.isWhitespace(charArray[i + STDOUT.length()]))) {
                copyOut = CopyOutToStdout.INSTANCE;
                break;
            } else { // 'filename'
                try {
                    final String constant;
                    constant = parseStringConstant(sql, charArray, i);
                    copyOut = program ? new CopyOutToProgramCommand(constant)
                            : new CopyOutToLocalFile(Paths.get(constant));
                    break;
                } catch (SQLException e) {
                    LOG.debug("COPY OUT TO clause parse filename error.", e);
                    String m = String.format("syntax error,TO clause error,at near %s", fragment(sql, i));
                    throw PgExceptions.createSyntaxError(m);
                }
            }

        } // for
        if (copyOut == null) {
            throw PgExceptions.createSyntaxError("Not Found TO clause in COPY COMMAND.");
        }
        return copyOut;
    }

    @Override
    public final boolean isSingleStmt(String sql) throws SQLException {
        return (Boolean) doParse(sql, Mode.CHECK_SINGLE);
    }

    /**
     * @see #parse(String)
     * @see #separateMultiStmt(String)
     * @see #parseCopyIn(String)
     */
    private Object doParse(final String multiStmt, final Mode mode) throws SQLException {
        final char[] charArray = multiStmt.toCharArray();
        final int lastIndex = charArray.length - 1;

        final boolean isTrace = LOG.isTraceEnabled();
        final long startMillis = isTrace ? System.currentTimeMillis() : 0;
        final boolean confirmStringOff = confirmStringIsOff();

        boolean inQuoteString = false, inCStyleEscapes = false, inUnicodeEscapes = false, inDoubleIdentifier = false;
        List<String> endpointList = new ArrayList<>();
        char ch;
        int lastEndpointEnd = 0, stmtCount = 1;
        loop:
        for (int i = 0; i < charArray.length; i++) {
            ch = charArray[i];

            if (inQuoteString) {
                final int index = multiStmt.indexOf(QUOTE, i);
                if (index < 0) {
                    throw createQuoteNotCloseError(multiStmt, i);
                }
                if ((confirmStringOff || inCStyleEscapes) && charArray[index - 1] == BACK_SLASH) {
                    // C-Style Escapes
                    i = index;
                } else if (index < lastIndex && charArray[index + 1] == QUOTE) {
                    // double quote Escapes
                    i = index + 1;
                } else {
                    i = index;
                    inQuoteString = false; // string constant end.
                    if (inUnicodeEscapes) {
                        inUnicodeEscapes = false;
                    } else if (inCStyleEscapes) {
                        inCStyleEscapes = false;
                    }
                }
            } else if (inDoubleIdentifier) {
                final int index = multiStmt.indexOf(DOUBLE_QUOTE, i);
                if (index < 0) {
                    throw createDoubleQuotedIdentifierError(multiStmt, i);
                }
                inDoubleIdentifier = false;
                if (inUnicodeEscapes) {
                    inUnicodeEscapes = false;
                }
                i = index;
            } else if (ch == SLASH && i < lastIndex && charArray[i + 1] == STAR) {
                // block comment.
                i = skipBlockComment(multiStmt, i);
            } else if (ch == DASH && i < lastIndex && charArray[i + 1] == DASH) {
                // line comment
                int index = multiStmt.indexOf('\n', i + DOUBLE_DASH_COMMENT_MARKER.length());
                i = index > 0 ? index : charArray.length;
            } else if (ch == QUOTE) {
                inQuoteString = true;
            } else if ((ch == 'E' || ch == 'e') && i < lastIndex && charArray[i + 1] == QUOTE) {
                inQuoteString = inCStyleEscapes = true;
                i++;
            } else if ((ch == 'U' || ch == 'u')
                    && i < lastIndex && charArray[i + 1] == '&'
                    && i + 2 < charArray.length && charArray[i + 2] == QUOTE) {
                inQuoteString = inUnicodeEscapes = true;
                i += 2;
            } else if (ch == DOUBLE_QUOTE) {
                inDoubleIdentifier = true;
            } else if ((ch == 'U' || ch == 'u')
                    && i < lastIndex && charArray[i + 1] == '&'
                    && i + 2 < charArray.length && charArray[i + 2] == DOUBLE_QUOTE) {
                inDoubleIdentifier = inUnicodeEscapes = true;
                i += 2;
            } else if (ch == DOLLAR) {
                // Dollar-Quoted String Constants
                int index = multiStmt.indexOf(DOLLAR, i + 1);
                if (index < 0) {
                    throw PgExceptions.createSyntaxError("syntax error at or near \"$\"");
                }
                final String dollarTag = multiStmt.substring(i, index + 1);
                index = multiStmt.indexOf(dollarTag, index + 1);
                if (index < 0) {
                    String msg = String.format(
                            "syntax error,Dollar-Quoted String Constants not close, at or near \"%s\"", dollarTag);
                    throw PgExceptions.createSyntaxError(msg);
                }
                i = index + dollarTag.length() - 1;
            } else if (ch == '?') {
                if (mode == Mode.BIND) {
                    endpointList.add(multiStmt.substring(lastEndpointEnd, i));
                    lastEndpointEnd = i + 1;
                }
            } else if (ch == ';') {
                switch (mode) {
                    case BIND:
                        String m = String.format(
                                "Detect multiple statements,multiple statements can't be bind,please check [%s]."
                                , multiStmt);
                        throw PgExceptions.createSyntaxError(m);
                    case SEPARATE:
                        endpointList.add(multiStmt.substring(lastEndpointEnd, i));
                        lastEndpointEnd = i + 1;
                        break;
                    case CHECK_SINGLE:
                        stmtCount++;
                        break loop; // break for
                    default:
                        throw PgExceptions.createUnexpectedEnumException(mode);
                }
            }

        } // for

        if (inQuoteString) {
            throw PgExceptions.createSyntaxError("syntax error,last string constants not close.");
        }
        if (inDoubleIdentifier) {
            throw PgExceptions.createSyntaxError("syntax error,last double quoted identifier not close.");
        }

        final Object parseResult;
        switch (mode) {
            case BIND: {
                if (lastEndpointEnd < charArray.length) {
                    endpointList.add(multiStmt.substring(lastEndpointEnd));
                } else {
                    endpointList.add("");
                }
                parseResult = PgStatementImpl.create(multiStmt, endpointList);
            }
            break;
            case CHECK_SINGLE:
                parseResult = stmtCount == 1;
                break;
            case SEPARATE: {
                if (lastEndpointEnd < charArray.length) {
                    endpointList.add(multiStmt.substring(lastEndpointEnd));
                }
                if (endpointList.size() == 1) {
                    parseResult = Collections.singletonList(endpointList.get(0));
                } else {
                    parseResult = Collections.unmodifiableList(endpointList);
                }
            }
            break;
            default:
                throw PgExceptions.createUnexpectedEnumException(mode);
        }
        if (isTrace) {
            LOG.trace("SQL[{}] \nparse cost {} ms.", multiStmt, System.currentTimeMillis() - startMillis);
        }
        return parseResult;
    }

    /**
     * @return first: index of {@code )} ; second: next bind index.
     * @see #parseCopyOut(String)
     */
    private Pair<Integer, Integer> skipCopyOutQuery(final String sql, final char[] charArray, int i, int bindIndex)
            throws SQLException {

        final Stack<Boolean> bracketStack = new FastStack<>();
        if (charArray[i] != '(') {
            throw new IllegalArgumentException("Not Query");
        }
        i++;
        bracketStack.push(Boolean.TRUE);


        final int lastIndex = charArray.length - 1;
        final boolean confirmStringOff = confirmStringIsOff();


        boolean inQuoteString = false, inCStyleEscapes = false, inUnicodeEscapes = false, inDoubleIdentifier = false;
        char ch;
        for (; i < charArray.length; i++) {
            ch = charArray[i];
            if (inQuoteString) {
                final int index = sql.indexOf(QUOTE, i);
                if (index < 0) {
                    throw createQuoteNotCloseError(sql, i);
                }
                if ((confirmStringOff || inCStyleEscapes) && charArray[index - 1] == BACK_SLASH) {
                    // C-Style Escapes
                    i = index;
                } else if (index < lastIndex && charArray[index + 1] == QUOTE) {
                    // double quote Escapes
                    i = index + 1;
                } else {
                    i = index;
                    inQuoteString = false; // string constant end.
                    if (inUnicodeEscapes) {
                        inUnicodeEscapes = false;
                    } else if (inCStyleEscapes) {
                        inCStyleEscapes = false;
                    }
                }
            } else if (inDoubleIdentifier) {
                final int index = sql.indexOf(DOUBLE_QUOTE, i);
                if (index < 0) {
                    throw createDoubleQuotedIdentifierError(sql, i);
                }
                inDoubleIdentifier = false;
                if (inUnicodeEscapes) {
                    inUnicodeEscapes = false;
                }
                i = index;
            } else if (ch == SLASH && i < lastIndex && charArray[i + 1] == STAR) {
                // block comment.
                i = skipBlockComment(sql, i);
            } else if (ch == DASH && i < lastIndex && charArray[i + 1] == DASH) {
                // line comment
                int index = sql.indexOf('\n', i + DOUBLE_DASH_COMMENT_MARKER.length());
                i = index > 0 ? index : charArray.length;
            } else if (ch == QUOTE) {
                inQuoteString = true;
            } else if ((ch == 'E' || ch == 'e') && i < lastIndex && charArray[i + 1] == QUOTE) {
                inQuoteString = inCStyleEscapes = true;
                i++;
            } else if ((ch == 'U' || ch == 'u')
                    && i < lastIndex && charArray[i + 1] == '&'
                    && i + 2 < charArray.length && charArray[i + 2] == QUOTE) {
                inQuoteString = inUnicodeEscapes = true;
                i += 2;
            } else if (ch == DOUBLE_QUOTE) {
                inDoubleIdentifier = true;
            } else if ((ch == 'U' || ch == 'u')
                    && i < lastIndex && charArray[i + 1] == '&'
                    && i + 2 < charArray.length && charArray[i + 2] == DOUBLE_QUOTE) {
                inDoubleIdentifier = inUnicodeEscapes = true;
                i += 2;
            } else if (ch == DOLLAR) {
                // Dollar-Quoted String Constants
                int index = sql.indexOf(DOLLAR, i + 1);
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
            } else if (ch == '?') {
                bindIndex++;
            } else if (ch == ')') {
                bracketStack.pop();
                if (bracketStack.isEmpty()) {
                    // query end.
                    break;
                }
            } else if (ch == '(') {
                bracketStack.push(Boolean.TRUE);
            }
        }
        return new Pair<>(i, bindIndex);
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
        return sql.substring(Math.max(index - 15, 0), Math.min(index + 15, sql.length()));
    }


    /**
     * @see #parseCopyIn(String)
     */
    private String parseStringConstant(final String sql, final char[] charArray, int i)
            throws SQLException {

        final String stringConstant;

        final char ch = charArray[i];
        if (ch == QUOTE) {
            // string constant
            final boolean confirmStringOff = confirmStringIsOff();
            stringConstant = parseQuoteStringConstant(sql, charArray, i, confirmStringOff);
        } else if ((ch == 'e' || ch == 'E')
                && charArray[i + 1] == QUOTE) {
            // string constant with c-style escapes.
            stringConstant = parseQuoteStringConstant(sql, charArray, i + 1, false);
        } else if ((ch == 'u' || ch == 'U')
                && charArray[i + 1] == '&'
                && charArray[i + 2] == QUOTE) {
            // string constant with unicode escapes.
            stringConstant = parseQuoteStringConstant(sql, charArray, i + 2, true);
        } else if (ch == DOLLAR) {
            // dollar-quoted string constant
            stringConstant = parseDollarQuotedStringConstant(sql, i);
        } else {
            String m = String.format("Not found string constant at or near \"%s\" .", fragment(sql, i));
            throw PgExceptions.createSyntaxError(m);
        }

        return stringConstant;
    }

    /**
     * @see #parseCopyIn(String)
     */
    private static String parseQuoteStringConstant(final String sql, final char[] charArray, final int i
            , final boolean recognizesBackslash)
            throws SQLException {
        final int lastIndex = charArray.length - 1;

        for (int j = i + 1; j < charArray.length; j++) {
            if (charArray[j] != QUOTE) {
                continue;
            }
            if (recognizesBackslash && charArray[j - 1] == BACK_SLASH) {
                // C-Style Escapes
                continue;
            } else if (j < lastIndex && charArray[j + 1] == QUOTE) {
                //  double quote Escapes
                j++;
                continue;
            }
            return sql.substring(i + 1, j);
        }
        throw createQuoteNotCloseError(sql, i);
    }

    /**
     * @see #parseCopyIn(String)
     */
    private static String parseDollarQuotedStringConstant(final String sql, final int i) throws SQLException {
        final int tagIndex = sql.indexOf('$', i + 1);
        if (tagIndex < 0) {
            throw createDollarQuotedStringConstantError(sql, i);
        }
        final int constantStartIndex = tagIndex + 1;
        final String tag = sql.substring(i, constantStartIndex);
        final int constantEndIndex = sql.indexOf(tag, constantStartIndex);
        if (constantEndIndex < 0) {
            throw createDollarQuotedNotCloseError(sql, i);
        }
        return sql.substring(constantStartIndex, constantEndIndex);
    }


    private static SQLException createQuoteNotCloseError(String sql, int index) {
        String m = String.format("Syntax error,string constants not close at near %s .", fragment(sql, index));
        return PgExceptions.createSyntaxError(m);
    }

    private static SQLException createDollarQuotedStringConstantError(String sql, int index) {
        String m = String.format("Dollar quoted string constant syntax error at or near \"%s\" .", fragment(sql, index));
        return PgExceptions.createSyntaxError(m);
    }

    private static SQLException createDollarQuotedNotCloseError(String sql, int index) {
        String m = String.format("Syntax error,dollar quoted string constant not close,at or near \"%s\" ."
                , fragment(sql, index));
        return PgExceptions.createSyntaxError(m);
    }

    private static SQLException createDoubleQuotedIdentifierError(String sql, int index) {
        String m = String.format(
                "syntax error,double quoted identifier not close,at near %s", fragment(sql, index));
        return PgExceptions.createSyntaxError(m);
    }

    private static IllegalArgumentException createBindIndexError(int bindIndex) {
        return new IllegalArgumentException(String.format("bindIndex[%s] less than 0", bindIndex));
    }

    private static IllegalStateException createCopyModeNotMatchError(Enum<?> mode) {
        return new IllegalStateException(String.format("Mode is %s", mode));
    }

    private static IllegalStateException createCopyExistsBindIndexError(int bindIndex) {
        return new IllegalStateException(String.format("Bind index[%s] great -1.", bindIndex));
    }

    private enum Mode {
        BIND,
        CHECK_SINGLE,
        SEPARATE
    }


    private static final class CopyInFromLocalFile implements CopyIn {

        private final Path path;

        private CopyInFromLocalFile(Path path) {
            this.path = path;
        }


        @Override
        public final Mode getMode() {
            return Mode.FILE;
        }

        @Override
        public final int getBindIndex() {
            return -1;
        }

        @Override
        public final Path getPath() {
            return this.path;
        }

        @Override
        public final String getCommand() {
            throw createCopyModeNotMatchError(getMode());
        }

    }

    private static final class CopyInFromLocalFileWithBind implements CopyIn {

        private final int bindIndex;

        private CopyInFromLocalFileWithBind(int bindIndex) {
            if (bindIndex < 0) {
                throw createBindIndexError(bindIndex);
            }
            this.bindIndex = bindIndex;
        }


        @Override
        public Mode getMode() {
            return Mode.FILE;
        }

        @Override
        public final int getBindIndex() {
            return this.bindIndex;
        }

        @Override
        public final Path getPath() {
            throw createCopyExistsBindIndexError(this.bindIndex);
        }

        @Override
        public final String getCommand() {
            throw createCopyModeNotMatchError(getMode());
        }

    }

    private static final class CopyInFromProgramCommand implements CopyIn {

        private final String command;

        private CopyInFromProgramCommand(String command) {
            this.command = command;
        }

        @Override
        public final Mode getMode() {
            return Mode.PROGRAM;
        }

        @Override
        public final int getBindIndex() {
            return -1;
        }

        @Override
        public final Path getPath() {
            throw createCopyModeNotMatchError(getMode());
        }

        @Override
        public final String getCommand() {
            return this.command;
        }
    }

    private static final class CopyInFromProgramCommandWithBind implements CopyIn {

        private final int bindIndex;

        private CopyInFromProgramCommandWithBind(int bindIndex) {
            if (bindIndex < 0) {
                throw createBindIndexError(bindIndex);
            }
            this.bindIndex = bindIndex;
        }

        @Override
        public final Mode getMode() {
            return Mode.PROGRAM;
        }

        @Override
        public final int getBindIndex() {
            return this.bindIndex;
        }

        @Override
        public final Path getPath() {
            throw createCopyModeNotMatchError(getMode());
        }

        @Override
        public final String getCommand() {
            throw createCopyExistsBindIndexError(this.bindIndex);
        }
    }

    private static final class CopyInFromStdin implements CopyIn {

        private static final CopyInFromStdin INSTANCE = new CopyInFromStdin();

        private CopyInFromStdin() {
        }

        @Override
        public final Mode getMode() {
            return Mode.STDIN;
        }

        @Override
        public final int getBindIndex() {
            return -1;
        }

        @Override
        public final Path getPath() {
            throw createCopyModeNotMatchError(getMode());
        }

        @Override
        public final String getCommand() {
            throw createCopyModeNotMatchError(getMode());
        }

    }

    private static final class CopyOutToLocalFile implements CopyOut {

        private final Path path;

        private CopyOutToLocalFile(Path path) {
            this.path = path;
        }


        @Override
        public final Mode getMode() {
            return Mode.FILE;
        }

        @Override
        public final int getBindIndex() {
            return -1;
        }

        @Override
        public final Path getPath() {
            return this.path;
        }

        @Override
        public final String getCommand() {
            throw createCopyModeNotMatchError(getMode());
        }

    }

    private static final class CopyOutToLocalFileWithBind implements CopyOut {

        private final int bindIndex;

        private CopyOutToLocalFileWithBind(int bindIndex) {
            if (bindIndex < 0) {
                throw new IllegalArgumentException("bindIndex less than 0");
            }
            this.bindIndex = bindIndex;
        }

        @Override
        public Mode getMode() {
            return Mode.FILE;
        }

        @Override
        public final int getBindIndex() {
            return this.bindIndex;
        }

        @Override
        public final Path getPath() {
            throw new IllegalStateException(String.format("Bind index[%s] great -1.", this.bindIndex));
        }

        @Override
        public final String getCommand() {
            throw createCopyModeNotMatchError(getMode());
        }

    }

    private static final class CopyOutToProgramCommand implements CopyOut {

        private final String command;

        private CopyOutToProgramCommand(String command) {
            this.command = command;
        }

        @Override
        public final Mode getMode() {
            return Mode.PROGRAM;
        }

        @Override
        public final int getBindIndex() {
            return -1;
        }

        @Override
        public final Path getPath() {
            throw createCopyModeNotMatchError(getMode());
        }

        @Override
        public final String getCommand() {
            return this.command;
        }
    }

    private static final class CopyOutToProgramCommandWithBind implements CopyOut {

        private final int bindIndex;

        private CopyOutToProgramCommandWithBind(int bindIndex) {
            if (bindIndex < 0) {
                throw new IllegalArgumentException("bindIndex less than 0");
            }
            this.bindIndex = bindIndex;
        }

        @Override
        public final Mode getMode() {
            return Mode.PROGRAM;
        }

        @Override
        public final int getBindIndex() {
            return this.bindIndex;
        }

        @Override
        public final Path getPath() {
            throw createCopyModeNotMatchError(getMode());
        }

        @Override
        public final String getCommand() {
            throw new IllegalStateException(String.format("bind index[%s] great -1.", this.bindIndex));
        }
    }

    private static final class CopyOutToStdout implements CopyOut {

        private static final CopyOutToStdout INSTANCE = new CopyOutToStdout();

        private CopyOutToStdout() {
        }

        @Override
        public final Mode getMode() {
            return Mode.STDOUT;
        }

        @Override
        public final int getBindIndex() {
            return -1;
        }

        @Override
        public final Path getPath() {
            throw createCopyModeNotMatchError(getMode());
        }

        @Override
        public final String getCommand() {
            throw createCopyModeNotMatchError(getMode());
        }


    }


}
