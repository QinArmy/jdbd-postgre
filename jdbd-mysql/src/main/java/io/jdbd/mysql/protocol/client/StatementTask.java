package io.jdbd.mysql.protocol.client;

interface StatementTask {

    int obtainStatementId();

    MySQLColumnMeta[] obtainParameterMetas();

    ClientProtocolAdjutant obtainAdjutant();

    int addAndGetSequenceId();

    boolean supportFetch();

}
