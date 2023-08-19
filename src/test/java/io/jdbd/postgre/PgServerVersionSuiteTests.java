package io.jdbd.postgre;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @see PgServerVersion
 */
@Test(groups = {Group.UTILS})
public class PgServerVersionSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgServerVersionSuiteTests.class);

    @Test
    public void version() {
        LOG.info("{} test start ", PgServerVersion.class.getName());


        assertEquals(PgServerVersion.INVALID.getVersion(), "0.0.0", "INVALID versionNum");
        assertEquals(PgServerVersion.INVALID.getVersionNumber(), 0, "INVALID versionNum");
        assertEquals(PgServerVersion.INVALID.getMajor(), 0, "INVALID major");
        assertEquals(PgServerVersion.INVALID.getMinor(), 0, "INVALID minor");
        assertEquals(PgServerVersion.INVALID.getSubMinor(), 0, "INVALID subMinor");

        assertEquals(PgServerVersion.V8_2.getVersion(), "8.2.0", "V8_2 versionNum");
        assertEquals(PgServerVersion.V8_2.getVersionNumber(), 80200, "V8_2 versionNum");
        assertEquals(PgServerVersion.V8_2.getMajor(), 8, "V8_2 major");
        assertEquals(PgServerVersion.V8_2.getMinor(), 2, "V8_2 minor");
        assertEquals(PgServerVersion.V8_2.getSubMinor(), 0, "V8_2 subMinor");


        assertEquals(PgServerVersion.V11.getVersion(), "11", "V11 versionNum");
        assertEquals(PgServerVersion.V11.getVersionNumber(), 110000, "V11 versionNum");
        assertEquals(PgServerVersion.V11.getMajor(), 11, "V11 major");
        assertEquals(PgServerVersion.V11.getMinor(), 0, "V11 minor");
        assertEquals(PgServerVersion.V11.getSubMinor(), 0, "V11 subMinor");


        final PgServerVersion maxVersion = PgServerVersion.from("99.99.99");

        assertEquals(maxVersion.getVersion(), "99.99.99", "maxVersion versionNum");
        assertEquals(maxVersion.getVersionNumber(), 999999, "maxVersion versionNum");
        assertEquals(maxVersion.getMajor(), 99, "maxVersion major");
        assertEquals(maxVersion.getMinor(), 99, "maxVersion minor");
        assertEquals(maxVersion.getSubMinor(), 99, "maxVersion subMinor");

        LOG.info("maxVersion:{}", maxVersion);
        LOG.info("{} test success ", PgServerVersion.class.getName());


    }


}
