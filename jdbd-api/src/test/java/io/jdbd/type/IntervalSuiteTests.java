package io.jdbd.type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.time.Duration;

import static org.testng.Assert.*;

/**
 * <p>
 * This class is a test class of {@link Interval}.
 * </p>
 */
public class IntervalSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(IntervalSuiteTests.class);


    /**
     * @see Interval#parse(String)
     */
    @Test
    public void parse() {
        String text, expectedText;
        Interval v;
        long second;

        // eg 1
        text = "PT0S";
        v = Interval.parse(text);
        assertEquals(v.toString(), text, text);

        // eg 2
        text = "P0D";
        v = Interval.parse(text);
        assertSame(v, Interval.ZERO, text);

        // eg 3
        text = "P1Y8M7DT98H23M22.999999999S";
        v = Interval.parse(text);
        assertEquals(v.getYears(), 1, text);
        assertEquals(v.getMonths(), 8, text);
        assertEquals(v.getDays(), 7, text);
        assertEquals(v.getSeconds(), (98L * 3600L + 23L * 60L + 22L), text);

        assertEquals(v.getNano(), 999_999_999L, text);
        assertEquals(v.toString(), text, text);

        // eg 4
        text = "P8MT98H23M22.333S";
        v = Interval.parse(text);
        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 8, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), (98L * 3600L + 23L * 60L + 22L), text);

        assertEquals(v.getNano(), 333_000_000L, text);
        assertEquals(v.toString(), text, text);

        // eg 5
        text = "P8MT98H23M-22.333S";
        v = Interval.parse(text);
        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 8, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), (98L * 3600L + 23L * 60L - 22L - 1L), text);
        second = v.getSeconds();
        assertEquals(v.getNano(), Interval.NANOS_PER_SECOND - 333_000_000L, text);
        expectedText = String.format("P8MT%sH%sM%s.%sS"
                , second / Interval.SECONDS_PER_HOUR
                , (second % Interval.SECONDS_PER_HOUR) / Interval.SECONDS_PER_MINUTE
                , second % Interval.SECONDS_PER_MINUTE, dropSuffixZero(v.getNano()));
        assertEquals(v.toString(), expectedText, text);

        // eg 6
        text = "PT-0.999999S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 0L, text);

        assertEquals(v.getNano(), -999_999_000L, text);
        assertEquals(v.toString(), text, text);

        // eg 7
        text = "PT-0.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 0L, text);

        assertEquals(v.getNano(), -900_000_000L, text);
        assertEquals(v.toString(), text, text);

        // eg 8
        text = "PT0.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 0L, text);

        assertEquals(v.getNano(), 900_000_000L, text);
        assertEquals(v.toString(), text, text);

        // eg 8
        text = "-PT0.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 0L, text);

        assertEquals(v.getNano(), -900_000_000L, text);
        assertEquals(v.toString(), "PT-0.9S", text);


        // eg 9
        text = "PT1M-60.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 0L, text);

        assertEquals(v.getNano(), -900_000_000L, text);
        assertEquals(v.toString(), "PT-0.9S", text);

        // eg 10
        text = "PT-1M-60.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), -120, text);

        assertEquals(v.getNano(), 900_000_000L, text);
        assertEquals(v.toString(), "PT-2M-0.9S", text);

        // eg 11
        text = "PT1M60.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 120, text);

        assertEquals(v.getNano(), 900_000_000L, text);
        assertEquals(v.toString(), "PT2M0.9S", text);

        // eg 12
        text = "PT2M-121.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), -1L, text);

        assertEquals(v.getNano(), 900_000_000L, text);
        assertEquals(v.toString(), "PT-1.9S", text);

        // eg 13
        text = "PT-2M121.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 1L, text);

        assertEquals(v.getNano(), 900_000_000L, text);
        assertEquals(v.toString(), "PT1.9S", text);

        // eg 14
        text = "PT2M-119.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 0L, text);

        assertEquals(v.getNano(), 100_000_000L, text);
        assertEquals(v.toString(), "PT0.1S", text);

        // eg 15
        text = "PT-2M119.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 0L, text);

        assertEquals(v.getNano(), -100_000_000L, text);
        assertEquals(v.toString(), "PT-0.1S", text);

        // eg 16
        text = "PT2M-119.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 0L, text);

        assertEquals(v.getNano(), 100_000_000L, text);
        assertEquals(v.toString(), "PT0.1S", text);


        // eg 17
        text = "PT2M-0.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 119L, text);

        assertEquals(v.getNano(), 100_000_000L, text);
        assertEquals(v.toString(), "PT1M59.1S", text);

        // eg 18
        text = "-PT-0.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), 0L, text);

        assertEquals(v.getNano(), 900_000_000L, text);
        assertEquals(v.toString(), "PT0.9S", text);

        // eg 19
        text = "-PT1M-0.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);
        assertEquals(v.getSeconds(), -59L, text);

        assertEquals(v.getNano(), 100_000_000L, text);
        assertEquals(v.toString(), "PT-59.1S", text);

        // eg 20
        text = "-P1Y-1M1DT-1H1M-0.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), -1, text);
        assertEquals(v.getMonths(), 1, text);
        assertEquals(v.getDays(), -1, text);
        assertEquals(v.getSeconds(), 3540L, text);

        assertEquals(v.getNano(), 900_000_000L, text);
        assertEquals(v.toString(), "P-1Y1M-1DT59M0.9S", text);

        // eg 21
        text = "-P-1Y1M-1DT1H-1M0.9S";
        v = Interval.parse(text);

        assertEquals(v.getYears(), 1, text);
        assertEquals(v.getMonths(), -1, text);
        assertEquals(v.getDays(), 1, text);
        assertEquals(v.getSeconds(), -3540L, text);

        assertEquals(v.getNano(), 900_000_000L, text);
        assertEquals(v.toString(), "P1Y-1M1DT-59M-0.9S", text);

    }

    /**
     * This is a but re-producer of {@link Duration#parse(CharSequence)}. If no error, {@link Duration#parse(CharSequence)} has bug.
     */
    @Test(enabled = false)
    public void durationBugReProducer() {
        //1.8.0_301
        System.out.println(Duration.class.getPackage().getImplementationVersion());
        String text;
        Duration d;


        text = "-PT-1H1M-0.9S";
        d = Duration.parse(text);
        assertNotEquals(d.toString(), "PT59M0.9S", text);


        text = "-PT1M-0.9S";
        d = Duration.parse(text);
        assertNotEquals(d.toString(), "PT-59.1S", text);

        text = "-PT-0.9S";
        d = Duration.parse(text);
        assertNotEquals(d.toString(), "PT0.9S", text);

        text = "PT-0.9S";
        d = Duration.parse(text);
        assertNotEquals(d.toString(), text, text);


    }

    /**
     * @see Interval#of(Duration)
     */
    @Test
    public void of() {
        Duration d;
        Interval v;
        String text;

        d = Duration.ofSeconds(0, 999_999_000);
        v = Interval.of(d);
        text = v.toString();

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);

        assertEquals(v.getSeconds(), 0L, text);
        assertEquals(v.getNano(), 999_999_000, text);
        assertEquals(v.toString(), "PT0.999999S", text);

        d = Duration.ofSeconds(0, 999_999_000).negated();
        v = Interval.of(d);
        text = v.toString();

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);

        assertEquals(v.getSeconds(), 0L, text);
        assertEquals(v.getNano(), -999_999_000, text);
        assertEquals(v.toString(), "PT-0.999999S", d.toString());


        d = Duration.ofSeconds(1, 999_999_000).negated();
        v = Interval.of(d);
        text = v.toString();

        assertEquals(v.getYears(), 0, text);
        assertEquals(v.getMonths(), 0, text);
        assertEquals(v.getDays(), 0, text);

        assertEquals(v.getSeconds(), -1L, text);
        assertEquals(v.getNano(), 999_999_000, text);
        assertEquals(text, "PT-1.999999S", d.toString());

    }


    private String dropSuffixZero(final int nano) {
        final String s = Integer.toString(nano);
        int nonZeroIndex = s.length();
        for (int i = s.length() - 1; i > -1; i--) {
            if (s.charAt(i) != '0') {
                nonZeroIndex = i + 1;
                break;
            }
        }
        return s.substring(0, nonZeroIndex);
    }

}
