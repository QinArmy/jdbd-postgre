package io.jdbd.mysql.protocol.client;

interface StatementTask {

    int obtainStatementId();

    MySQLColumnMeta[] obtainParameterMetas();

    ClientProtocolAdjutant obtainAdjutant();

    void startSafeSequenceId();

    void endSafeSequenceId();

    int safelyAddAndGetSequenceId();

    boolean supportFetch();

}
