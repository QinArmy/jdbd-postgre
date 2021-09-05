package io.jdbd.type;

import java.time.Duration;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.time.temporal.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class Interval implements TemporalAmount {

    private final Period period;

    private final Duration duration;

    private Interval(Period period, Duration duration) {
        this.period = period;
        this.duration = duration;
    }

    @Override
    public final long get(final TemporalUnit unit) {
        final long value;
        if (unit == ChronoUnit.YEARS
                || unit == ChronoUnit.MONTHS
                || unit == ChronoUnit.DAYS) {
            value = this.period.get(unit);
        } else if (unit == ChronoUnit.SECONDS
                || unit == ChronoUnit.NANOS) {
            value = this.duration.get(unit);
        } else {
            throw new UnsupportedTemporalTypeException("Unsupported unit: " + unit);
        }
        return value;
    }

    @Override
    public final List<TemporalUnit> getUnits() {
        return UnitHolder.UNIT_LIST;
    }

    @Override
    public final Temporal addTo(Temporal temporal) {
        return this.duration.addTo(this.period.addTo(temporal));

    }

    @Override
    public final Temporal subtractFrom(Temporal temporal) {
        return this.duration.subtractFrom(this.period.subtractFrom(temporal));
    }

    @Override
    public final int hashCode() {
        return Objects.hash(this.period, this.duration);
    }

    @Override
    public final boolean equals(Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof Interval) {
            final Interval interval = (Interval) obj;
            match = this.period.equals(interval.period) && this.duration.equals(interval.duration);
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String toString() {
        return this.period.toString() + this.duration.toString().substring(1);
    }

    public static Interval of(Period period, Duration duration) {
        return new Interval(period, duration);
    }

    public static Interval of(Duration duration) {
        return new Interval(Period.ZERO, duration);
    }

    public static Interval of(Period period) {
        return new Interval(period, Duration.ZERO);
    }

    /**
     * @throws DateTimeParseException if the text cannot be parsed to a IntervalPair
     */
    public static Interval parse(final String textValue) throws DateTimeParseException {
        final Interval pair;
        int timeIndex;
        if ((timeIndex = textValue.indexOf('T')) < 0
                && (timeIndex = textValue.indexOf('t')) < 0) {
            pair = of(Period.parse(textValue));
        } else if (timeIndex == 1) {
            pair = of(Duration.parse(textValue));
        } else if (timeIndex > 1) {
            Period period = Period.parse(textValue.substring(0, timeIndex));
            Duration duration = Duration.parse("P" + textValue.substring(timeIndex));
            pair = Interval.of(period, duration);
        } else {
            String m = String.format("TextValue[%s] isn't iso interval.", textValue);
            throw new DateTimeParseException(m, textValue, timeIndex);
        }
        return pair;
    }


    private static final class UnitHolder {

        private static final List<TemporalUnit> UNIT_LIST = Collections.unmodifiableList(
                Arrays.asList(ChronoUnit.YEARS, ChronoUnit.MONTHS
                        , ChronoUnit.DAYS, ChronoUnit.SECONDS
                        , ChronoUnit.NANOS));


    }


}
