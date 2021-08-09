package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;

interface StmtTask {

     void addError(JdbdException error);

     TaskAdjutant adjutant();
}
