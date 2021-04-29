package io.jdbd.postgre;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @see ServerVersion
 */
public class ServerVersionSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ServerVersionSuiteTests.class);

    @Test
    public void version() {
        LOG.info("{} test start ", ServerVersion.class.getName());


        assertEquals(ServerVersion.INVALID.getCompleteVersion(), "0.0.0", "INVALID versionNum");
        assertEquals(ServerVersion.INVALID.getVersion(), 0, "INVALID versionNum");
        assertEquals(ServerVersion.INVALID.getMajor(), 0, "INVALID major");
        assertEquals(ServerVersion.INVALID.getMinor(), 0, "INVALID minor");
        assertEquals(ServerVersion.INVALID.getSubMinor(), 0, "INVALID subMinor");

        assertEquals(ServerVersion.V8_2.getCompleteVersion(), "8.2.0", "V8_2 versionNum");
        assertEquals(ServerVersion.V8_2.getVersion(), 80200, "V8_2 versionNum");
        assertEquals(ServerVersion.V8_2.getMajor(), 8, "V8_2 major");
        assertEquals(ServerVersion.V8_2.getMinor(), 2, "V8_2 minor");
        assertEquals(ServerVersion.V8_2.getSubMinor(), 0, "V8_2 subMinor");


        assertEquals(ServerVersion.V11.getCompleteVersion(), "11", "V11 versionNum");
        assertEquals(ServerVersion.V11.getVersion(), 110000, "V11 versionNum");
        assertEquals(ServerVersion.V11.getMajor(), 11, "V11 major");
        assertEquals(ServerVersion.V11.getMinor(), 0, "V11 minor");
        assertEquals(ServerVersion.V11.getSubMinor(), 0, "V11 subMinor");


        final ServerVersion maxVersion = ServerVersion.from("99.99.99");

        assertEquals(maxVersion.getCompleteVersion(), "99.99.99", "maxVersion versionNum");
        assertEquals(maxVersion.getVersion(), 999999, "maxVersion versionNum");
        assertEquals(maxVersion.getMajor(), 99, "maxVersion major");
        assertEquals(maxVersion.getMinor(), 99, "maxVersion minor");
        assertEquals(maxVersion.getSubMinor(), 99, "maxVersion subMinor");

        LOG.info("maxVersion:{}", maxVersion);
        LOG.info("{} test success ", ServerVersion.class.getName());


    }


}
