package io.jdbd.pool;

import io.jdbd.JdbdException;
import io.jdbd.stmt.Stmt;



public interface PoolAdvice {

      Stmt beforeExecutor(Stmt stmt)throws JdbdException;



}
