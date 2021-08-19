package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;

@Test(groups = {Group.TASK_TEST_ADVICE}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS})
public class TaskTestAdvice extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(TaskTestAdvice.class);

    @BeforeGroups()
    public void beforeTaskTest() {

    }

    @Test
    public void insertTestData() {

    }


}
