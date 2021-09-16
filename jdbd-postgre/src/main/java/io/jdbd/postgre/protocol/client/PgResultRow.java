package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.result.UnsupportedConvertingException;
import io.jdbd.type.Interval;
import io.jdbd.vendor.result.AbstractResultRow;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.DateTimeException;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;

public class PgResultRow extends AbstractResultRow<PgRowMeta> {

    static PgResultRow create(PgRowMeta rowMeta, Object[] columnValues, TaskAdjutant adjutant) {
        return new PgResultRow(rowMeta, columnValues, adjutant);
    }

    private final TaskAdjutant adjutant;

    private PgResultRow(PgRowMeta rowMeta, Object[] columnValues, TaskAdjutant adjutant) {
        super(rowMeta, columnValues);
        this.adjutant = adjutant;
    }


    @Override
    protected final BigDecimal convertToBigDecimal(final int indexBaseZero, final Object nonNull) {
        final BigDecimal value;
        if (nonNull instanceof String) {
            switch (this.rowMeta.columnMetaArray[indexBaseZero].pgType) {
                case MONEY:
                case MONEY_ARRAY:
                    value = convertMoneyToBigDecimal(indexBaseZero, (String) nonNull);
                    break;
                default:
                    value = super.convertToBigDecimal(indexBaseZero, nonNull);
            }
        } else {
            value = super.convertToBigDecimal(indexBaseZero, nonNull);
        }
        return value;
    }


    @Override
    protected UnsupportedConvertingException createValueCannotConvertException(Throwable cause, int indexBasedZero, Class<?> targetClass) {
        return null;
    }

    @Override
    protected ZoneOffset obtainZoneOffsetClient() {
        return null;
    }

    @Override
    protected final Charset obtainColumnCharset(int indexBasedZero) {
        return this.adjutant.clientCharset();
    }

    @Override
    protected final TemporalAccessor convertStringToTemporalAccessor(int indexBaseZero, String sourceValue
            , Class<?> targetClass) throws DateTimeException, UnsupportedConvertingException {
        return null;
    }

    @Override
    protected final TemporalAmount convertStringToTemporalAmount(int indexBaseZero, String nonNull, Class<?> targetClass)
            throws DateTimeException, UnsupportedConvertingException {
        return Interval.parse(nonNull);
    }

    @Override
    protected String formatTemporalAccessor(TemporalAccessor temporalAccessor) throws DateTimeException {
        return null;
    }


    /**
     * @see #convertToBigDecimal(int, Object)
     */
    private BigDecimal convertMoneyToBigDecimal(final int indexBaseZero, final String nonNull)
            throws UnsupportedConvertingException {
        final DecimalFormat format = this.rowMeta.moneyFormat;
        if (format == null) {
            throw moneyCannotConvertException(indexBaseZero);
        }
        try {
            final Number value;
            value = format.parse(nonNull);
            if (!(value instanceof BigDecimal)) {
                throw moneyCannotConvertException(indexBaseZero);
            }
            return (BigDecimal) value;
        } catch (Throwable e) {
            final PgType pgType = this.rowMeta.columnMetaArray[indexBaseZero].pgType;
            String msg = String.format("Column[%s] postgre %s type convert to  java type BigDecimal occur failure,%s"
                    , this.rowMeta.getColumnLabel(indexBaseZero)
                    , pgType, e.getMessage());
            throw new UnsupportedConvertingException(msg, e, pgType, BigDecimal.class);
        }
    }


    private UnsupportedConvertingException moneyCannotConvertException(int indexBasedZero) {
        PgType pgType = this.rowMeta.columnMetaArray[indexBasedZero].pgType;
        String format;
        format = "%s.getCurrencyInstance(Locale) method don't return %s instance,so can't convert postgre %s type to java type BigDecimal,jdbd-postgre need to upgrade.";
        String msg = String.format(format
                , NumberFormat.class.getName()
                , DecimalFormat.class.getName()
                , pgType);
        return new UnsupportedConvertingException(msg, pgType, BigDecimal.class);
    }


}
