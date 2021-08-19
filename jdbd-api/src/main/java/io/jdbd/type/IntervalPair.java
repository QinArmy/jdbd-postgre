package io.jdbd.type;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class IntervalPair implements TemporalAmount {

    private final Period period;

    private final Duration duration;

    private IntervalPair(Period period, Duration duration) {
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


    public static IntervalPair of(Period period, Duration duration) {
        return new IntervalPair(period, duration);
    }

    public static IntervalPair of(Duration duration) {
        return new IntervalPair(Period.ZERO, duration);
    }

    public static IntervalPair of(Period period) {
        return new IntervalPair(period, Duration.ZERO);
    }


    private static final class UnitHolder {

        private static final List<TemporalUnit> UNIT_LIST = Collections.unmodifiableList(
                Arrays.asList(ChronoUnit.YEARS, ChronoUnit.MONTHS
                        , ChronoUnit.DAYS, ChronoUnit.SECONDS
                        , ChronoUnit.NANOS));


    }


}
