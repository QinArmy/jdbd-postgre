package io.jdbd.vendor.util;

import io.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @see JdbdBinds
 */
public class JdbdBindsSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(JdbdBindsSuiteTests.class);


    /**
     * @see JdbdArrays#getArrayDimensions(Class)
     */
    @Test
    public void getArrayDimensions() {
        Pair<Class<?>, Integer> pair;
        Class<?> arrayClass, componentClass;

        arrayClass = Integer[].class;
        componentClass = Integer.class;
        pair = JdbdArrays.getArrayDimensions(arrayClass);
        assertEquals(pair.getFirst(), componentClass, arrayClass.getName());
        assertEquals(pair.getSecond(), Integer.valueOf(1), arrayClass.getName());

        arrayClass = Integer[][].class;
        componentClass = Integer.class;
        pair = JdbdArrays.getArrayDimensions(arrayClass);
        assertEquals(pair.getFirst(), componentClass, arrayClass.getName());
        assertEquals(pair.getSecond(), Integer.valueOf(2), arrayClass.getName());

        arrayClass = int[].class;
        componentClass = int.class;
        pair = JdbdArrays.getArrayDimensions(arrayClass);
        assertEquals(pair.getFirst(), componentClass, arrayClass.getName());
        assertEquals(pair.getSecond(), Integer.valueOf(1), arrayClass.getName());

        arrayClass = int[][][].class;
        componentClass = int.class;
        pair = JdbdArrays.getArrayDimensions(arrayClass);
        assertEquals(pair.getFirst(), componentClass, arrayClass.getName());
        assertEquals(pair.getSecond(), Integer.valueOf(3), arrayClass.getName());

        LOG.info("getArrayDimensions test success.");

    }


}
