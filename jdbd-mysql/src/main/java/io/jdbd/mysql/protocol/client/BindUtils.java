package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.BatchBindStmt;
import io.jdbd.mysql.stmt.BindableStmt;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLNumbers;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.util.JdbdBinds;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Queue;

abstract class BindUtils extends JdbdBinds {

    protected BindUtils() {
        throw new UnsupportedOperationException();
    }


    static boolean useBatchPrepare(BatchBindStmt stmt, TaskAdjutant adjutant) {
        //TODO zoro ,fill code
        return false;
    }

    static boolean usePrepare(BindableStmt stmt, TaskAdjutant adjutant) {
        //TODO zoro ,fill code
        return false;
    }


    static long bindToBits(final int stmtIndex, MySQLType mySQLType, ParamValue bindValue
            , Charset clientCharset)
            throws JdbdSQLException {
        final Object nonNull = bindValue.getNonNull();

        final long bits;
        if (nonNull instanceof Long) {
            bits = (Long) nonNull;
        } else if (nonNull instanceof Integer
                || nonNull instanceof Short
                || nonNull instanceof Byte) {
            bits = ((Number) nonNull).longValue();
        } else {

            final byte[] bytes;
            if (nonNull instanceof String) {
                bytes = ((String) nonNull).getBytes(clientCharset);
            } else if (nonNull instanceof BigDecimal) {
                bytes = ((BigDecimal) nonNull).toPlainString().getBytes(clientCharset);
            } else if (nonNull instanceof BigInteger) {
                bytes = nonNull.toString().getBytes(clientCharset);
            } else if (nonNull instanceof byte[]) {
                bytes = (byte[]) nonNull;
            } else {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, mySQLType, bindValue);
            }
            if (bytes.length == 0) {
                throw MySQLExceptions.createWrongArgumentsException(stmtIndex, mySQLType, bindValue, null);
            } else if (bytes.length < 9) {
                bits = MySQLNumbers.readLongFromBigEndian(bytes, 0, bytes.length);
            } else {
                throw MySQLExceptions.createDataTooLongException(stmtIndex, mySQLType, bindValue);
            }
        }
        return bits;
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
