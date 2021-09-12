package io.jdbd.vendor.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.BitSet;
import java.util.Random;

import static org.testng.Assert.assertEquals;

/**
 * This class is a test class of {@link JdbdStrings}
 */
public class JdbdStringsSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(JdbdStringsSuiteTests.class);


    /**
     * @see JdbdStrings#bitSetToBitString(BitSet, boolean)
     */
    @Test
    public void bitSetToBitString() {
        BitSet bitSet;
        String bitString, original, reverseOriginal;

        final Random random = new Random();
        long next;
        for (int i = 0; i < 100; i++) {
            next = random.nextLong();
            original = Long.toBinaryString(next);
            reverseOriginal = new StringBuilder(original).reverse().toString();

            bitSet = BitSet.valueOf(new long[]{next});

            bitString = JdbdStrings.bitSetToBitString(bitSet, false);
            assertEquals(bitString, reverseOriginal, original);

            bitString = JdbdStrings.bitSetToBitString(bitSet, true);
            assertEquals(bitString, original, original);

        }


    }

    /**
     * @see JdbdStrings#bitStringToBitSet(String, boolean)
     */
    @Test
    public void bitStringToBitSet() {
        BitSet bitSet;
        String original, reverseOriginal;

        final Random random = new Random();
        long next;
        for (int i = 0; i < 100; i++) {
            next = random.nextLong();
            original = Long.toBinaryString(next);
            reverseOriginal = new StringBuilder(original).reverse().toString();

            bitSet = JdbdStrings.bitStringToBitSet(original, true);
            assertEquals(bitSet, BitSet.valueOf(new long[]{next}), original);

            bitSet = JdbdStrings.bitStringToBitSet(reverseOriginal, false);
            assertEquals(bitSet, BitSet.valueOf(new long[]{next}), original);

        }

    }

    /**
     * @see JdbdStrings#bitStringToBitSet(String, boolean)
     * @see JdbdStrings#bitSetToBitString(BitSet, boolean)
     */
    @Test(dependsOnMethods = {"bitSetToBitString", "bitStringToBitSet"})
    public void bitStringAndBitSetConvert() {
        BitSet bitSet;
        String bitString, original, reverseOriginal;

        final Random random = new Random();
        long next;
        for (int i = 0; i < 100; i++) {
            next = random.nextLong();
            original = Long.toBinaryString(next);
            reverseOriginal = new StringBuilder(original).reverse().toString();

            bitSet = JdbdStrings.bitStringToBitSet(original, true);

            bitString = JdbdStrings.bitSetToBitString(bitSet, true);
            assertEquals(bitString, original, original);

            bitString = JdbdStrings.bitSetToBitString(bitSet, false);
            assertEquals(bitString, reverseOriginal, original);

        }

    }


}
