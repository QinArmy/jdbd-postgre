package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.postgre.type.PgBox;
import io.jdbd.postgre.type.PgGeometries;
import io.jdbd.postgre.type.PgLine;
import io.jdbd.postgre.type.PgPolygon;
import io.jdbd.result.ResultRow;
import io.jdbd.result.UnsupportedConvertingException;
import io.jdbd.type.Interval;
import io.jdbd.type.geo.Line;
import io.jdbd.type.geo.LineString;
import io.jdbd.type.geometry.Circle;
import io.jdbd.vendor.result.AbstractResultRow;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
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
    protected final <T> T convertToOther(final int indexBaseZero, final Object nonNull, final Class<T> targetClass)
            throws UnsupportedConvertingException {
        final T value;
        try {
            final PgType pgType = this.rowMeta.columnMetaArray[indexBaseZero].pgType;
            if (targetClass == PgLine.class) {
                value = targetClass.cast(PgGeometries.line(nonNull.toString()));
            } else if (targetClass == Line.class) {
                value = targetClass.cast(PgGeometries.lineSegment(nonNull.toString()));
            } else if (targetClass == PgBox.class) {
                value = targetClass.cast(PgGeometries.box(nonNull.toString()));
            } else if (targetClass == LineString.class && pgType == PgType.PATH) {
                value = targetClass.cast(PgGeometries.path(nonNull.toString()));
            } else if (targetClass == PgPolygon.class && pgType == PgType.POLYGON) {
                value = targetClass.cast(PgGeometries.polygon(nonNull.toString()));
            } else if (targetClass == Circle.class && pgType == PgType.CIRCLE) {
                value = targetClass.cast(PgGeometries.circle(nonNull.toString()));
            } else {
                value = super.convertToOther(indexBaseZero, nonNull, targetClass);
            }
            return value;
        } catch (IllegalArgumentException e) {
            throw createValueCannotConvertException(e, indexBaseZero, targetClass);
        }
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
            final String columnLabel = this.rowMeta.getColumnLabel(indexBaseZero);
            String m;
            m = String.format("Column[%s] postgre %s type convert to  java type BigDecimal failure."
                    , columnLabel
                    , pgType);
            if (e instanceof ParseException) {
                m = String.format("%s\nYou couldn't execute '%s' AND %s.get(\"%s\",%s.class) in multi-statement."
                        , m
                        , "SET lc_monetary"
                        , ResultRow.class.getName()
                        , columnLabel
                        , BigDecimal.class.getName()
                );
            }
            throw new UnsupportedConvertingException(m, e, pgType, BigDecimal.class);
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
