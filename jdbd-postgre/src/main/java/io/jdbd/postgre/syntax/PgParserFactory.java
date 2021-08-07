package io.jdbd.postgre.syntax;

public abstract class PgParserFactory {

    private PgParserFactory() {
        throw new UnsupportedOperationException();
    }


    public static PgParser forBeforeAuthentication() {
        return null;
    }


}
