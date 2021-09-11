package io.jdbd.postgre.protocol.client;

import io.jdbd.result.UnsupportedConvertingException;
import io.jdbd.vendor.result.AbstractResultRow;

import java.nio.charset.Charset;
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
    protected UnsupportedConvertingException createNotSupportedException(int indexBasedZero, Class<?> targetClass) {
        return null;
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
    protected Charset obtainColumnCharset(int indexBasedZero) {
        return null;
    }

    @Override
    protected final TemporalAccessor convertStringToTemporalAccessor(int indexBaseZero, String sourceValue
            , Class<?> targetClass) throws DateTimeException, UnsupportedConvertingException {
        return null;
    }

    @Override
    protected TemporalAmount convertStringToTemporalAmount(int indexBaseZero, String sourceValue, Class<?> targetClass) throws DateTimeException, UnsupportedConvertingException {
        return null;
    }

    @Override
    protected String formatTemporalAccessor(TemporalAccessor temporalAccessor) throws DateTimeException {
        return null;
    }


}
