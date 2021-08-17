package io.jdbd.postgre.protocol.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * @see PgResultStates
 */
public class PgResultStatesSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgResultStatesSuiteTests.class);

    /**
     * @see PgResultStates#create(int, boolean, String)
     */
    @Test
    public void create() {
        PgResultStates states;
        String commandTag;

        commandTag = "INSERT 1 1";

        states = PgResultStates.create(0, false, commandTag);
        assertEquals(states.getResultIndex(), 0, commandTag);
        assertEquals(states.getAffectedRows(), 1L, commandTag);
        assertEquals(states.getInsertId(), 1L, commandTag);

        assertFalse(states.hasReturningColumn(), commandTag);
        assertFalse(states.hasMoreResult(), commandTag);


        commandTag = "UPDATE 10";

        states = PgResultStates.create(0, true, commandTag);
        assertEquals(states.getResultIndex(), 0, commandTag);
        assertEquals(states.getAffectedRows(), 10L, commandTag);
        assertEquals(states.getInsertId(), 0L, commandTag);

        assertFalse(states.hasReturningColumn(), commandTag);
        assertTrue(states.hasMoreResult(), commandTag);

        commandTag = "SET";

        states = PgResultStates.create(1, true, commandTag);
        assertEquals(states.getResultIndex(), 1, commandTag);
        assertEquals(states.getAffectedRows(), 0L, commandTag);
        assertEquals(states.getInsertId(), 0L, commandTag);

        assertFalse(states.hasReturningColumn(), commandTag);
        assertTrue(states.hasMoreResult(), commandTag);


        commandTag = "SELECT 22";

        states = PgResultStates.create(1, true, commandTag);
        assertEquals(states.getResultIndex(), 1, commandTag);
        assertEquals(states.getAffectedRows(), 22L, commandTag);
        assertEquals(states.getInsertId(), 0L, commandTag);

        assertTrue(states.hasReturningColumn(), commandTag);
        assertTrue(states.hasMoreResult(), commandTag);

    }


}
