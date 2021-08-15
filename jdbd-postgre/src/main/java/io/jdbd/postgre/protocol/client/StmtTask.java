package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;

interface StmtTask {

     void addResultSetError(JdbdException error);

     TaskAdjutant adjutant();

     int getAndIncrementResultIndex();

}
