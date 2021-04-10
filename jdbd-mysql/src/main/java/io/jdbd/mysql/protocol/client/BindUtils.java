package io.jdbd.mysql.protocol.client;

import io.jdbd.BindParameterException;
import io.jdbd.mysql.BindValue;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.util.JdbdBindUtils;
import reactor.util.annotation.Nullable;

import java.sql.SQLException;

abstract class BindUtils extends JdbdBindUtils {

    protected BindUtils() {
        throw new UnsupportedOperationException();
    }


    public static String bindToBits(final int stmtIndex, final BindValue bindValue) throws SQLException {
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
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
        }
        return ("B'" + bits + "'");
    }


    /*################################## blow private exception ##################################*/

    static BindParameterException createTypeNotMatchException(BindValue bindValue) {
        return createTypeNotMatchException(bindValue, null);
    }

    static BindParameterException createTypeNotMatchException(BindValue bindValue, @Nullable Throwable cause) {
        return new BindParameterException(cause, bindValue.getParamIndex()
                , "Bind parameter[%s] MySQLType[%s] and JavaType[%s] value not match."
                , bindValue.getParamIndex()
                , bindValue.getType()
                , bindValue.getRequiredValue().getClass().getName()
        );
    }

    static BindParameterException createNotSupportFractionException(BindValue bindValue) {
        throw new BindParameterException(String.format("Bind parameter[%s] is MySQLType[%s],not support fraction."
                , bindValue.getParamIndex()
                , bindValue.getType())
                , bindValue.getParamIndex());
    }

    static BindParameterException createNumberRangErrorException(BindValue bindValue, Number lower
            , Number upper) {
        return new BindParameterException(String.format("Bind parameter[%s] MySQLType[%s] beyond rang[%s,%s]."
                , bindValue.getParamIndex(), bindValue.getType(), lower, upper)
                , bindValue.getParamIndex());

    }

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
}
