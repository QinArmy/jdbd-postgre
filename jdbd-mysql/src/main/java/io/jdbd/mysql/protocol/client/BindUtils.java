package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.statement.ParamValue;
import io.jdbd.vendor.util.JdbdBindUtils;
import io.netty.buffer.ByteBuf;

import java.sql.SQLException;
import java.util.Queue;

abstract class BindUtils extends JdbdBindUtils {

    protected BindUtils() {
        throw new UnsupportedOperationException();
    }


    public static String bindToBits(final int stmtIndex, MySQLType mySQLType, final ParamValue bindValue)
            throws SQLException {
        final Object nonNullValue = bindValue.getRequiredValue();

        final String bits;
        if (nonNullValue instanceof Long) {
            bits = Long.toBinaryString((Long) nonNullValue);
        } else if (nonNullValue instanceof Integer) {
            bits = Long.toBinaryString((Integer) nonNullValue & 0xFFFF_FFFFL);
        } else if (nonNullValue instanceof Short) {
            bits = Long.toBinaryString((Short) nonNullValue & 0xFFFF);
        } else if (nonNullValue instanceof Byte) {
            bits = Long.toBinaryString((Byte) nonNullValue & 0xFF);
        } else if (nonNullValue instanceof byte[]) {
            final byte[] bytes = (byte[]) nonNullValue;
            StringBuilder builder = new StringBuilder(bytes.length * 8);
            for (byte b : bytes) {
                for (int i = 0; i < 8; i++) {
                    builder.append((b & (1 << i)) != 0 ? 1 : 0);
                }
            }
            bits = builder.toString();
        } else if (nonNullValue instanceof String) {
            try {
                bits = Long.toBinaryString(Long.parseLong((String) nonNullValue, 2));
            } catch (NumberFormatException e) {
                //TODO optimize exception
                throw MySQLExceptions.createInvalidParameterNoError(stmtIndex, bindValue.getParamIndex());
            }
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, mySQLType, bindValue);
        }
        return ("B'" + bits + "'");
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
