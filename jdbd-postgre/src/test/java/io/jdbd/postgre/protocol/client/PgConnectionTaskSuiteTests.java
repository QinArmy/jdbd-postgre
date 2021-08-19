package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * @see PgConnectionTask
 */
@Test(groups = {Group.SESSION_BUILDER}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS})
public class PgConnectionTaskSuiteTests extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgConnectionTaskSuiteTests.class);


    @Test
    public void authentication() {
        LOG.info("passwordAuthentication test start.");
        ClientProtocolFactory.single(DEFAULT_SESSION_ADJUTANT, 0)
                .flatMap(ClientProtocol::close)
                .block();
        LOG.info("passwordAuthentication test end.");
    }


}
