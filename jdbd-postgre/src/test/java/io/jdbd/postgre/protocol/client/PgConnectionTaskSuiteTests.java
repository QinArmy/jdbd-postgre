package io.jdbd.postgre.protocol.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * @see PgConnectionTask
 */
public class PgConnectionTaskSuiteTests extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgConnectionTaskSuiteTests.class);


    @Test
    public void authentication() {
        LOG.info("passwordAuthentication test start.");
        ClientProtocol protocol = obtainProtocol();
        LOG.info("passwordAuthentication test end.");
        releaseConnection(protocol);
    }


}
