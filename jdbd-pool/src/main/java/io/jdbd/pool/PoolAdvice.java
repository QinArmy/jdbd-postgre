package io.jdbd.pool;

import io.jdbd.JdbdException;
import io.jdbd.stmt.Stmt;


public interface PoolAdvice {

      void beforeExecute(Stmt stmt) throws JdbdException;

}
