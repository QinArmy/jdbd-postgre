package io.jdbd.mysql.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class SimpleTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTests.class);

    @Test
    public void zoneOffset() {
        final ZoneId localZoneId = ZoneId.systemDefault();

        ZoneOffset offset = ZoneOffset.of("+04:18");
        ZonedDateTime now = ZonedDateTime.now(offset);
        LOG.info("now : {}", now);
        Instant instant = now.toInstant();

        ZoneOffset localOffset = localZoneId.getRules().getOffset(instant);
        LOG.info("localZoneId : {} , localOffset :{}", localZoneId, localOffset);
    }

    @Test
    public void simple() {
        System.out.println(Integer.toBinaryString(67108864).length());
    }

}
