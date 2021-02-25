package io.jdbd.vendor.statement;

import java.util.List;
import java.util.function.Supplier;

public abstract class SQLParserBuilder {

    String blockCommentStartMarker;

    String blockCommentEndMarker;

    List<String> lineCommentMarkers;

    Supplier<Boolean> backslashEscapes;

    List<Character> quotedIdentifierChars;

    public SQLParserBuilder setBlockCommentStartMarker(String blockCommentStartMarker) {
        this.blockCommentStartMarker = blockCommentStartMarker;
        return this;
    }

    public SQLParserBuilder setBlockCommentEndMarker(String blockCommentEndMarker) {
        this.blockCommentEndMarker = blockCommentEndMarker;
        return this;
    }

    public SQLParserBuilder setLineCommentMarkers(List<String> lineCommentMarkers) {
        this.lineCommentMarkers = lineCommentMarkers;
        return this;
    }

    public SQLParserBuilder setBackslashEscapes(Supplier<Boolean> backslashEscapes) {
        this.backslashEscapes = backslashEscapes;
        return this;
    }

    public SQLParserBuilder setQuotedIdentifierChars(List<Character> quotedIdentifierChars) {
        this.quotedIdentifierChars = quotedIdentifierChars;
        return this;
    }

    public abstract SQLParser build();

}
