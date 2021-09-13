package io.jdbd.type;

import reactor.util.annotation.Nullable;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.time.temporal.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class Interval implements TemporalAmount {

    public static final Interval ZERO = new Interval(0, 0, 0, 0L, 0);


    private final int years;

    private final int months;

    private final int days;

    private final long seconds;

    /**
     * The number of nanoseconds in the duration, expressed as a fraction of the
     * number of seconds. This is always positive, and never exceeds 999,999,999.
     */
    private final int nano;


    private Interval(int years, int months, int days, long seconds, final int nano) {
        if (nano < 0 || nano > 999_999_999) {
            throw new IllegalArgumentException("nano error");
        }
        this.years = years;
        this.months = months;
        this.days = days;
        this.seconds = seconds;
        this.nano = nano;
    }


    @Override
    public final long get(final TemporalUnit unit) {
        if (!(unit instanceof ChronoUnit)) {
            throw new UnsupportedTemporalTypeException("Unsupported unit: " + unit);
        }
        final long value;
        switch ((ChronoUnit) unit) {
            case YEARS:
                value = this.years;
                break;
            case MONTHS:
                value = this.months;
                break;
            case DAYS:
                value = this.days;
                break;
            case SECONDS:
                value = this.seconds;
                break;
            case NANOS:
                value = this.nano;
                break;
            default:
                throw new UnsupportedTemporalTypeException("Unsupported unit: " + unit);

        }
        return value;
    }

    @Override
    public final List<TemporalUnit> getUnits() {
        final List<TemporalUnit> unitList;
        if (this == ZERO) {
            unitList = Collections.emptyList();
        } else if ((this.years | this.months | this.days) == 0) {
            unitList = TimeUnitHolder.UNIT_LIST;
        } else if ((this.seconds | this.nano) == 0) {
            unitList = DateUnitHolder.UNIT_LIST;
        } else {
            unitList = AllUnitHolder.UNIT_LIST;
        }
        return unitList;
    }


    public final boolean isZero() {
        return this == ZERO;
    }

    @Override
    public final Temporal addTo(Temporal temporal) {
        if (this.years != 0) {
            temporal = temporal.plus(this.years, ChronoUnit.YEARS);
        }
        if (this.months != 0) {
            temporal = temporal.plus(this.months, ChronoUnit.MONTHS);
        }
        if (this.days != 0) {
            temporal = temporal.plus(this.days, ChronoUnit.DAYS);
        }
        if (this.seconds != 0) {
            temporal = temporal.plus(this.seconds, ChronoUnit.SECONDS);
        }
        if (this.nano != 0) {
            if (this.seconds < 0) {
                temporal = temporal.minus(this.nano, ChronoUnit.NANOS);
            } else {
                temporal = temporal.plus(this.nano, ChronoUnit.NANOS);
            }
        }
        return temporal;

    }

    @Override
    public final Temporal subtractFrom(Temporal temporal) {
        if (this.years != 0) {
            temporal = temporal.minus(this.years, ChronoUnit.YEARS);
        }
        if (this.months != 0) {
            temporal = temporal.minus(this.months, ChronoUnit.MONTHS);
        }
        if (this.days != 0) {
            temporal = temporal.minus(this.days, ChronoUnit.DAYS);
        }
        if (this.seconds != 0) {
            temporal = temporal.minus(this.seconds, ChronoUnit.SECONDS);
        }
        if (this.nano != 0) {
            if (this.seconds < 0) {
                temporal = temporal.plus(this.nano, ChronoUnit.NANOS);
            } else {
                temporal = temporal.minus(this.nano, ChronoUnit.NANOS);
            }
        }
        return temporal;
    }


    @Override
    public final int hashCode() {
        return Objects.hash(this.years, this.months, this.days, this.seconds, this.nano);
    }

    @Override
    public final boolean equals(Object obj) {
        return equals(obj, false);
    }

    public final boolean equals(Object obj, final boolean microPrecision) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof Interval) {
            final Interval interval = (Interval) obj;
            final boolean m = interval.years == this.years
                    && interval.months == this.months
                    && interval.days == this.days
                    && interval.seconds == this.seconds;
            if (microPrecision) {
                match = m && (interval.nano / 1000 == this.nano / 1000);
            } else {
                match = m && interval.nano == this.nano;
            }
        } else {
            match = false;
        }
        return match;
    }

    public final Duration toDurationExact() throws DateTimeException {
        if ((this.years | this.months | this.days) != 0) {
            throw new DateTimeException(String.format("[%s] can't convert to %s .", this, Duration.class.getName()));
        }
        final Duration duration;
        if (this.seconds > 0) {
            duration = Duration.ofSeconds(this.seconds, this.nano);
        } else {
            duration = Duration.ofSeconds(Math.abs(this.seconds), this.nano).negated();
        }
        return duration;
    }

    public final Period toPeriodExact() throws DateTimeException {
        if ((this.seconds | this.nano) != 0) {
            throw new DateTimeException(String.format("[%s] can't convert to %s .", this, Duration.class.getName()));
        }
        return Period.of(this.years, this.months, this.days);
    }

    @Override
    public final String toString() {
        return toString(false);
    }

    public String toString(final boolean microPrecision) {
        if (this == ZERO) {
            return "PT0S";
        }
        final StringBuilder builder = new StringBuilder(40);
        builder.append('P');
        if (this.years != 0) {
            builder.append(this.years)
                    .append('Y');
        }
        if (this.months != 0) {
            builder.append(this.months)
                    .append('M');
        }
        if (this.days != 0) {
            builder.append(this.days)
                    .append('D');
        }
        if ((this.seconds | this.nano) != 0) {
            final long hours = this.seconds / 3600L, minutes = (this.seconds % 3600L) / 60L, sec = this.seconds % 60L;
            builder.append('T');
            if (hours != 0) {
                builder.append(hours)
                        .append('H');
            }
            if (minutes != 0) {
                builder.append(minutes)
                        .append('M');
            }
            if ((sec | this.nano) != 0) {
                builder.append(sec);
                if (this.nano > 0) {
                    builder.append('.');
                    final String n = Integer.toString(microPrecision ? (this.nano / 1000) : this.nano);
                    final int siteCount = (microPrecision ? 6 : 9) - n.length();
                    for (int i = 0; i < siteCount; i++) {
                        builder.append('0');
                    }
                    builder.append(n);
                }
                builder.append('S');
            }

        }
        return builder.toString();
    }

    public static Interval of(Period period, Duration duration) {
        duration = duration.isNegative() ? duration.negated() : duration;
        return create(period.getYears(), period.getMonths(), period.getDays()
                , duration.getSeconds(), duration.getNano());
    }

    public static Interval of(Duration duration) {
        return of(Period.ZERO, duration);
    }

    public static Interval of(Period period) {
        return of(period, Duration.ZERO);
    }

    /**
     * @throws DateTimeParseException if the text cannot be parsed to a IntervalPair
     */
    public static Interval parse(final String textValue)
            throws DateTimeParseException {
        return parse(textValue, false);
    }


    public static Interval parse(final String text, final boolean microPrecision)
            throws DateTimeParseException {
        final int length = text.length();
        if (length < 3) {
            throw new DateTimeParseException(String.format("[%s] length less than 3.", text), text, 0);
        }
        final boolean negativePrefix = text.charAt(0) == '-';

        if (negativePrefix && text.charAt(1) != 'P') {
            throw new DateTimeParseException(String.format("[%s] format error.", text), text, 1);
        } else if (!negativePrefix && text.charAt(0) != 'P') {
            throw new DateTimeParseException(String.format("[%s] format error.", text), text, 0);
        }
        final int pIndex = negativePrefix ? 1 : 0;
        final int tIndex = text.indexOf('T', pIndex + 1);
        final int[] datePart;
        final long[] timePart;
        if (tIndex < 0) {
            datePart = parseDatePart(negativePrefix, text, pIndex, length);
            timePart = new long[]{0L, 0L, 0L, 0L};
        } else if (tIndex == pIndex + 1) {
            datePart = new int[]{0, 0, 0};
            timePart = parseTimePart(negativePrefix, text, tIndex, microPrecision);
        } else {
            datePart = parseDatePart(negativePrefix, text, pIndex, tIndex);
            timePart = parseTimePart(negativePrefix, text, tIndex, microPrecision);
        }
        if (datePart.length != 3 || timePart.length != 4) {
            throw new IllegalStateException("error");
        }

        long seconds = timePart[2];
        seconds = Math.addExact(Math.multiplyExact(timePart[0], 3600L), seconds);
        seconds = Math.addExact(Math.multiplyExact(timePart[1], 60L), seconds);
        return create(datePart[0], datePart[1], datePart[2], seconds, (int) timePart[3]);
    }


    /**
     * @param startIndex index of 'T'
     */
    private static long[] parseTimePart(boolean negativePrefix, final String text, final int startIndex
            , final boolean microPrecision) {
        if (text.charAt(startIndex) != 'T') {
            throw new IllegalArgumentException(String.format("startIndex[%s] error.", startIndex));
        }

        final char[] keyChars = new char[]{'H', 'M'};
        final long[] quantity = new long[keyChars.length + 2];
        final int length = text.length();

        int from = startIndex + 1, to;

        try {
            for (int i = 0; i < keyChars.length; i++) {
                to = text.indexOf(keyChars[i], from);
                if (to < 0) {
                    quantity[i] = 0L;
                } else {
                    final long temp = Long.parseLong(text.substring(from, to));
                    quantity[i] = negativePrefix ? Math.negateExact(temp) : temp;
                    from = to + 1;
                    if (from == length) {
                        break;
                    }
                }

            }
            if (from < length) {
                final int sIndex = text.indexOf('S', from);
                if (sIndex != length - 1) {
                    throw createCannotParseError(text, from, null);
                }
                final int pointIndex = text.indexOf('.', from);
                if (pointIndex < 0) {
                    final long temp = Long.parseLong(text.substring(from, sIndex));
                    quantity[2] = negativePrefix ? Math.negateExact(temp) : temp;
                    quantity[3] = 0L;
                } else {
                    final long temp = Long.parseLong(text.substring(from, pointIndex));
                    quantity[2] = negativePrefix ? Math.negateExact(temp) : temp;
                    final long nano = Long.parseLong(text.substring(pointIndex + 1, sIndex));
                    if (nano < 0 || (microPrecision && nano > 999_999) || (!microPrecision && nano > 999_999_999)) {
                        throw createCannotParseError(text, pointIndex + 1, null);
                    }
                    quantity[3] = nano;
                }
            }
            return quantity;
        } catch (NumberFormatException | ArithmeticException e) {
            throw createCannotParseError(text, from, e);
        }
    }

    /**
     * @param startIndex index of 'P'
     * @param endIndex   index of 'T' or length of text.
     */
    private static int[] parseDatePart(boolean negativePrefix, final String text, final int startIndex
            , final int endIndex)
            throws DateTimeParseException {

        if (text.charAt(startIndex) != 'P') {
            throw new IllegalArgumentException(String.format("startIndex[%s] error.", startIndex));
        }

        final char[] keyChars = new char[]{'Y', 'M', 'D'};
        final int[] quantity = new int[keyChars.length];

        int from = startIndex + 1;
        try {
            for (int i = 0, to; i < keyChars.length; i++) {
                to = text.indexOf(keyChars[i], from);
                if (to < 0) {
                    quantity[i] = 0;
                } else if (to < endIndex) {
                    final int temp = Integer.parseInt(text.substring(from, to));
                    quantity[i] = negativePrefix ? Math.negateExact(temp) : temp;
                    from = to + 1;
                    if (from >= endIndex) {
                        break;
                    }
                } else if (to > endIndex) {
                    if (i != 1) {
                        throw createCannotParseError(text, from, null);
                    }
                    quantity[i] = 0;
                } else {
                    throw new IllegalArgumentException(String.format("endIndex[%s] error.", endIndex));
                }
            }
            return quantity;
        } catch (NumberFormatException e) {
            throw createCannotParseError(text, from, e);
        }
    }

    private static Interval create(final int years, final int months, final int days
            , final long seconds, final int nano) {
        final Interval v;
        if ((years | months | days | seconds | nano) == 0) {
            v = ZERO;
        } else {
            v = new Interval(years, months, days, seconds, nano);
        }
        return v;
    }

    private static DateTimeParseException createCannotParseError(String text, int from, @Nullable Throwable cause) {
        String msg = String.format("[%s] can't parse at index[%s].", text, from);
        return new DateTimeParseException(msg, text, from, cause);
    }


    private static final class DateUnitHolder {

        private static final List<TemporalUnit> UNIT_LIST = Collections.unmodifiableList(
                Arrays.asList(ChronoUnit.YEARS, ChronoUnit.MONTHS
                        , ChronoUnit.DAYS));

    }

    private static final class TimeUnitHolder {

        private static final List<TemporalUnit> UNIT_LIST = Collections.unmodifiableList(
                Arrays.asList(ChronoUnit.SECONDS
                        , ChronoUnit.NANOS));

    }

    private static final class AllUnitHolder {

        private static final List<TemporalUnit> UNIT_LIST = Collections.unmodifiableList(
                Arrays.asList(ChronoUnit.YEARS, ChronoUnit.MONTHS
                        , ChronoUnit.DAYS, ChronoUnit.SECONDS
                        , ChronoUnit.NANOS));


    }


}
