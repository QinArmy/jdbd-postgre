package io.jdbd.mysql.util;

import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.vendor.stmt.Value;
import io.jdbd.vendor.util.JdbdBinds;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;

import java.time.Year;
import java.util.Queue;
import java.util.Set;

public abstract class MySQLBinds extends JdbdBinds {


    @Nullable
    public static MySQLType handleDataType(final DataType dataType) {
        return null;
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/year.html">The YEAR Type</a>
     */
    public static int bindToYear(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNull();
        final int value;
        if (nonNull instanceof Year) {
            value = ((Year) nonNull).getValue();
        } else if (nonNull instanceof Short) {
            value = (Short) nonNull;
        } else if (nonNull instanceof Integer) {
            value = (Integer) nonNull;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }

        if (value > 2155 || (value < 1901 && value != 0)) {
            throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
        }
        return value;
    }

    public static String bindToSetType(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNull();
        if (nonNull instanceof String) {
            return (String) nonNull;
        }
        if (!(nonNull instanceof Set)) {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        final Set<?> set = (Set<?>) nonNull;
        final StringBuilder builder = new StringBuilder(set.size() * 4);
        int index = 0;
        for (Object element : set) {
            if (index > 0) {
                builder.append(',');
            }
            if (element instanceof String) {
                builder.append(element);
            } else if (element instanceof Enum) {
                builder.append(((Enum<?>) element).name());
            } else {
                throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
            }
            index++;
        }
        return builder.toString();
    }


    public static void assertParamCountMatch(int stmtIndex, int paramCount, int bindCount) {

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

    public static void releaseOnError(Queue<ByteBuf> queue, final ByteBuf packet) {
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
