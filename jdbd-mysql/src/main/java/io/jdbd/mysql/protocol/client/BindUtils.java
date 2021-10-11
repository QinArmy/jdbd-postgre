package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.stmt.BindBatchStmt;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.util.JdbdBinds;
import io.netty.buffer.ByteBuf;

import java.sql.SQLException;
import java.util.Queue;

abstract class BindUtils extends JdbdBinds {

    protected BindUtils() {
        throw new UnsupportedOperationException();
    }


    static boolean useBatchPrepare(BindBatchStmt stmt, TaskAdjutant adjutant) {
        //TODO zoro ,fill code
        return false;
    }

    static boolean usePrepare(BindStmt stmt, TaskAdjutant adjutant) {
        //TODO zoro ,fill code
        return false;
    }


    /*################################## blow private exception ##################################*/

    static void assertParamCountMatch(int stmtIndex, int paramCount, int bindCount)
            throws SQLException {

        if (bindCount != paramCount) {
            if (paramCount == 0) {
                throw MySQLExceptions.createNoParametersExistsError(stmtIndex);
            } else if (paramCount > bindCount) {
                throw MySQLExceptions.createParamsNotBindError(stmtIndex, bindCount);
            } else {
                throw MySQLExceptions.createInvalidParameterNoError(stmtIndex, paramCount);
            }
        }
    }


    static void releaseOnError(Queue<ByteBuf> queue, final ByteBuf packet) {
        ByteBuf byteBuf;
        while ((byteBuf = queue.poll()) != null) {
            byteBuf.release();
        }
        queue.clear();
        if (packet.refCnt() > 0) {
            packet.release();
        }
    }


}
