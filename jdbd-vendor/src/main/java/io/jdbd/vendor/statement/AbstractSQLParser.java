package io.jdbd.vendor.statement;

import io.jdbd.JdbdSQLException;
import io.jdbd.vendor.util.JdbdStringUtils;

import java.util.List;
import java.util.function.Supplier;

public abstract class AbstractSQLParser implements SQLParser {

    private String blockCommentStartMarker;

    private String blockCommentEndMarker;

    private Supplier<List<String>> lineCommentMarkersFunc;

    private Supplier<Boolean> backslashEscapesFunc;

    private Supplier<List<Character>> quotedIdentifierCharsFunc;

    private boolean commentMarkerCaseSense;


    protected AbstractSQLParser(SQLParserBuilder builder) {

    }

    @Override
    public final SQLStatement parse(final String singleSql) throws JdbdSQLException {
        if (!JdbdStringUtils.hasLength(singleSql)) {
            throw createEmptySqlException();
        }
        final boolean backslashEscapes = this.backslashEscapesFunc.get();
        final String blockCommentStartMarker = this.blockCommentStartMarker;
        final String blockCommentEndMarker = this.blockCommentEndMarker;

        boolean inQuotes = false;
        boolean inQuotedId = false;

        final int sqlLength = singleSql.length();

        char ch;
        for (int i = 0; i < sqlLength; i++) {
            ch = singleSql.charAt(i);
            if (Character.isWhitespace(ch)) {
                continue;
            }


        }
        return null;
    }


    protected abstract JdbdSQLException createEmptySqlException();


}
