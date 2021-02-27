package io.jdbd.mysql.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.vendor.TaskAdjutant;

interface MySQLTaskAdjutant extends TaskAdjutant, ClientProtocolAdjutant, MySQLParser {


    MySQLCommTaskExecutor obtainCommTaskExecutor();

    @Nullable
    @Override
    Integer getServerStatus() throws JdbdMySQLException;



}
